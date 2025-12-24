import asyncio
import random
import aiohttp
from typing import Optional, Dict, Set
from aiohttp import web
from urllib.parse import quote, unquote

from async_ssdp import SSDPService, ParsedMessage, ParsedMessageType
from upnp_to_json import UPnPConverter


class DiscoveredDevice:
    """Represents a discovered UPnP device"""
    
    def __init__(self, location: str, device_type: str, uuid: str, addr: tuple):
        self.location = location
        self.device_type = device_type
        self.uuid = uuid
        self.addr = addr
        self.last_seen = asyncio.get_event_loop().time()
    
    def update_last_seen(self):
        self.last_seen = asyncio.get_event_loop().time()


class JSONUPnPProxyServer:
    """
    JSON-UPnP Proxy Server
    
    This server acts as a bridge between JSON-mode SSDP clients and traditional XML UPnP devices.
    
    Features:
    - Discovers traditional UPnP devices via SSDP
    - Advertises itself as a json-upnp proxy
    - Converts XML device descriptions to JSON on-demand
    - Caches converted descriptions
    - Provides API endpoints for clients to query devices
    """
    
    def __init__(self,
                 host: str,
                 port: int,
                 proxy_uuid: Optional[str] = None,
                 cache_ttl: int = 3600,
                 multicast_group: str = '224.0.0.1',
                 multicast_port: int = 5007,
                 announce_interval: int = 600):
        
        self.host = host
        self.port = port
        self.cache_ttl = cache_ttl
        self.announce_interval = announce_interval
        
        # Generate UUID if not provided
        import uuid
        self.proxy_uuid = proxy_uuid or str(uuid.uuid4())
        
        # Device registry
        self.devices: Dict[str, DiscoveredDevice] = {}  # uuid -> DiscoveredDevice
        
        # Conversion cache
        self.conversion_cache: Dict[str, Dict] = {}  # location URL -> converted JSON
        
        # SSDP service for proxy announcement
        location = f"http://{host}:{port}/proxy/description"
        self.ssdp = SSDPService(
            device="json-upnp-proxy",
            uuid=self.proxy_uuid,
            location=location,
            cache=1800,
            json_upnp=True,  # Advertise as json-upnp
            multicast_group=multicast_group,
            multicast_port=multicast_port
        )
        
        # HTTP server components
        self.app = None
        self.runner = None
        self.site = None
        
        # Background tasks
        self.announce_task = None
        self.discovery_task = None
        self.cleanup_task = None
    
    async def start(self):
        """Start the proxy server"""
        # Start HTTP server
        await self._start_http_server()
        
        # Start SSDP listener
        await self.ssdp.start_listening()
        
        # Subscribe to all SSDP messages to discover devices
        self.ssdp.subscribe(self._handle_ssdp_message, 
                          [ParsedMessageType.NOTIFY, ParsedMessageType.RESPONSE])
        
        # Subscribe to M-SEARCH requests to respond as proxy
        self.ssdp.subscribe(self._handle_msearch, [ParsedMessageType.MSEARCH])
        
        # Start periodic announcements
        self.announce_task = asyncio.create_task(self._announce_loop())
        
        # Start periodic discovery
        self.discovery_task = asyncio.create_task(self._discovery_loop())
        
        # Start cleanup task
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        print(f"JSON-UPnP Proxy Server started:")
        print(f"  - Proxy endpoint: http://{self.host}:{self.port}")
        print(f"  - Proxy UUID: {self.proxy_uuid}")
        print(f"  - Mode: json-upnp")
    
    async def stop(self):
        """Stop the proxy server"""
        # Cancel background tasks
        for task in [self.announce_task, self.discovery_task, self.cleanup_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Send byebye
        await self.ssdp.broadcast_byebye()
        
        # Stop SSDP
        await self.ssdp.stop_listening()
        
        # Stop HTTP server
        await self._stop_http_server()
        
        print("JSON-UPnP Proxy Server stopped")
    
    # ========================================================================
    # HTTP Server
    # ========================================================================
    
    async def _start_http_server(self):
        """Start the HTTP server"""
        self.app = web.Application()
        
        # Proxy description endpoint
        self.app.router.add_get('/proxy/description', self._handle_proxy_description)
        
        # Device conversion endpoints
        self.app.router.add_get('/device/json', self._handle_device_to_json)
        self.app.router.add_get('/device/xml', self._handle_device_passthrough)
        
        # Device registry endpoints
        self.app.router.add_get('/devices', self._handle_list_devices)
        self.app.router.add_get('/devices/{uuid}', self._handle_get_device)
        
        # Start server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
    
    async def _stop_http_server(self):
        """Stop the HTTP server"""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
    
    async def _handle_proxy_description(self, request):
        """Return JSON description of the proxy itself"""
        description = {
            "deviceType": "urn:schemas-json-upnp-org:device:proxy:1",
            "friendlyName": "JSON-UPnP Proxy Server",
            "manufacturer": "Python SSDP Package",
            "modelName": "json-upnp-proxy",
            "modelNumber": "1.0",
            "UDN": f"uuid:{self.proxy_uuid}",
            "services": [
                {
                    "serviceType": "urn:schemas-json-upnp-org:service:DeviceProxy:1",
                    "serviceId": "urn:upnp-org:serviceId:DeviceProxy",
                    "description": "Converts XML UPnP devices to JSON",
                    "endpoints": {
                        "convert": f"http://{self.host}:{self.port}/device/json?url={{device_location}}",
                        "list": f"http://{self.host}:{self.port}/devices",
                        "get": f"http://{self.host}:{self.port}/devices/{{uuid}}"
                    }
                }
            ],
            "presentationURL": f"http://{self.host}:{self.port}/devices"
        }
        return web.json_response(description)
    
    async def _handle_device_to_json(self, request):
        """Convert a device's XML description to JSON"""
        url = request.rel_url.query.get("url")
        if not url:
            return web.json_response({"error": "Missing 'url' parameter"}, status=400)
        
        url = unquote(url)
        
        # Check cache
        if url in self.conversion_cache:
            return web.json_response(self.conversion_cache[url])
        
        try:
            # Fetch XML from device
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return web.json_response(
                            {"error": f"Device returned status {resp.status}"}, 
                            status=502
                        )
                    
                    xml_text = await resp.text()
            
            # Convert to JSON
            json_dict = UPnPConverter.xml_to_dict(xml_text, doc_type='device')
            
            # Cache it
            self.conversion_cache[url] = json_dict
            
            return web.json_response(json_dict)
            
        except asyncio.TimeoutError:
            return web.json_response({"error": "Timeout fetching device description"}, status=504)
        except Exception as e:
            print(f"Error converting device: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    async def _handle_device_passthrough(self, request):
        """Pass through XML description without conversion"""
        url = request.rel_url.query.get("url")
        if not url:
            return web.Response(text="Missing 'url' parameter", status=400)
        
        url = unquote(url)
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    xml_text = await resp.text()
                    return web.Response(text=xml_text, content_type='text/xml')
        except Exception as e:
            return web.Response(text=str(e), status=500)
    
    async def _handle_list_devices(self, request):
        """List all discovered devices"""
        devices_list = []
        for uuid, device in self.devices.items():
            devices_list.append({
                "uuid": uuid,
                "location": device.location,
                "deviceType": device.device_type,
                "addr": f"{device.addr[0]}:{device.addr[1]}",
                "lastSeen": device.last_seen,
                "jsonUrl": f"http://{self.host}:{self.port}/device/json?url={quote(device.location)}"
            })
        
        return web.json_response({"devices": devices_list, "count": len(devices_list)})
    
    async def _handle_get_device(self, request):
        """Get specific device by UUID"""
        uuid = request.match_info['uuid']
        
        if uuid not in self.devices:
            return web.json_response({"error": "Device not found"}, status=404)
        
        device = self.devices[uuid]
        
        # Try to fetch and convert device description
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(device.location, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    xml_text = await resp.text()
            
            json_dict = UPnPConverter.xml_to_dict(xml_text, doc_type='device')
            
            return web.json_response({
                "uuid": uuid,
                "location": device.location,
                "addr": f"{device.addr[0]}:{device.addr[1]}",
                "lastSeen": device.last_seen,
                "description": json_dict
            })
        except Exception as e:
            return web.json_response({
                "uuid": uuid,
                "location": device.location,
                "error": str(e)
            }, status=500)
    
    # ========================================================================
    # SSDP Handlers
    # ========================================================================
    
    async def _handle_ssdp_message(self, message: ParsedMessage, addr):
        """Handle SSDP discovery messages (NOTIFY/RESPONSE)"""
        location = message.get_location()
        uuid = message.get_uuid()
        device_type = message.get_role()
        
        if not location or not uuid:
            return
        
        # Skip messages from our proxy
        if uuid == self.proxy_uuid:
            return
        
        # Skip json-upnp devices (they don't need conversion)
        if device_type and 'json-upnp' in device_type.lower():
            return
        
        # Register or update device
        if uuid in self.devices:
            self.devices[uuid].update_last_seen()
        else:
            self.devices[uuid] = DiscoveredDevice(location, device_type or "unknown", uuid, addr)
            print(f"Discovered device: {device_type} at {location}")
    
    async def _handle_msearch(self, message: ParsedMessage, addr):
        """Handle M-SEARCH requests - respond as proxy"""
        target = message.get_search_target()
        
        # Only respond if they're searching for json-upnp or all devices
        should_respond = False
        
        if target == "ssdp:all":
            should_respond = True
        elif target and "json-upnp" in target.lower():
            should_respond = True
        elif target and "proxy" in target.lower():
            should_respond = True
        
        if should_respond:
            mx = message.get_max_wait() or 3
            delay = random.uniform(0, mx)
            
            await asyncio.sleep(delay)
            await self.ssdp.broadcast_msearch_response(target)
    
    # ========================================================================
    # Background Tasks
    # ========================================================================
    
    async def _announce_loop(self):
        """Periodically announce proxy presence"""
        try:
            await self.ssdp.broadcast_alive()
            
            while True:
                await asyncio.sleep(self.announce_interval)
                await self.ssdp.broadcast_alive()
        except asyncio.CancelledError:
            pass
    
    async def _discovery_loop(self):
        """Periodically search for new devices"""
        try:
            # Initial delay
            await asyncio.sleep(5)
            
            while True:
                # Search for all UPnP devices
                await self.ssdp.broadcast_msearch("ssdp:all", mx=3)
                
                # Wait before next search
                await asyncio.sleep(120)  # Every 2 minutes
        except asyncio.CancelledError:
            pass
    
    async def _cleanup_loop(self):
        """Remove stale devices from registry"""
        try:
            while True:
                await asyncio.sleep(300)  # Every 5 minutes
                
                current_time = asyncio.get_event_loop().time()
                stale_devices = []
                
                for uuid, device in self.devices.items():
                    # Remove devices not seen in last hour
                    if current_time - device.last_seen > 3600:
                        stale_devices.append(uuid)
                
                for uuid in stale_devices:
                    print(f"Removing stale device: {uuid}")
                    del self.devices[uuid]
                
                # Clear old cache entries
                if len(self.conversion_cache) > 100:
                    # Keep only the most recent 50
                    items = list(self.conversion_cache.items())
                    self.conversion_cache = dict(items[-50:])
        except asyncio.CancelledError:
            pass


# ============================================================================
# Example Usage
# ============================================================================

async def example_usage():
    """Example of running the JSON-UPnP Proxy Server"""
    
    # Create and start proxy
    proxy = JSONUPnPProxyServer(
        host="192.168.1.63",
        port=5030,
        announce_interval=600
    )
    
    await proxy.start()
    
    print("\nProxy is now:")
    print("  1. Discovering traditional UPnP devices")
    print("  2. Advertising itself as json-upnp-proxy")
    print("  3. Ready to convert XML to JSON on request")
    print("\nClient usage:")
    print(f"  - List devices: http://192.168.1.63:5030/devices")
    print(f"  - Convert device: http://192.168.1.63:5030/device/json?url=<device_location>")
    
    # Run until interrupted
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await proxy.stop()


if __name__ == "__main__":
    asyncio.run(example_usage())