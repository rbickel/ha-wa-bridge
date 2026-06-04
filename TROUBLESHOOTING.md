# Troubleshooting WhatsApp Bridge Connection Issues

This guide helps diagnose and resolve connection stability issues with the WhatsApp bridge.

## Connection Health Monitoring

### Home Assistant Integration Logs

The HA integration now provides detailed connection metrics in the logs:

```
Connecting to WhatsApp Bridge at ws://localhost:3000 (attempt after 0 failures)
Connected to WhatsApp Bridge
```

On disconnection, you'll see:
```
WebSocket closed by server after 123.4 seconds
Reconnecting in 5 seconds... (consecutive failures: 1, total reconnects: 1)
```

With exponential backoff, the retry delay increases on consecutive failures:
- 1st failure: 5 seconds
- 2nd failure: 10 seconds
- 3rd failure: 20 seconds
- 4th failure: 40 seconds
- Maximum: 300 seconds (5 minutes)

### Bridge Service Logs

#### Memory Monitoring

The bridge logs memory usage every 60 seconds:

```
[MEMORY #1] RSS=245MB  Heap=120/150MB  External=25MB  Uptime=60s
```

Watch for memory warnings:
```
[MEMORY WARNING] RSS usage is high: 850MB (limit: 1GB)
```

#### Connection Status

The bridge logs detailed disconnect information:

```
Connection closed – reason code: 428
  Logged out: false
  Restart required: false
  Connection lost: true
  Timed out: false
[MEMORY] RSS=245MB at disconnect (uptime: 3600s)
Exiting with code 1 to trigger container restart (uptime: 3600s)
```

#### WebSocket Client Tracking

```
New client connected (total clients: 1)
Client disconnected (remaining clients: 0)
Broadcast status to 1 client(s)
```

## Common Issues and Solutions

### Issue: Frequent Disconnects (Every Few Minutes)

**Symptoms:**
- Bridge disconnects every 2-10 minutes
- Pattern: `Connection reset by peer` or `Server disconnected`
- HA reconnects successfully after 5-10 seconds

**Diagnosis:**

1. Check bridge memory usage:
   ```bash
   docker stats ha-whatsapp-bridge
   ```

2. Check bridge logs for disconnect reason:
   ```bash
   docker logs ha-whatsapp-bridge --tail 100
   ```

**Solutions:**

1. **Memory Issues**: If RSS is consistently near the limit (>80%):
   - For Baileys: Increase `mem_limit` in docker-compose.yml from 1g to 1.5g
   - For whatsapp-web.js: Increase from 2g to 3g

   ```yaml
   mem_limit: 1.5g  # Increase as needed
   ```

2. **WhatsApp Rate Limiting**:
   - Reduce message frequency
   - Enable `INCOMING_MESSAGES_MODE=disabled` if you only send messages:
     ```yaml
     environment:
       - INCOMING_MESSAGES_MODE=disabled
     ```

3. **Network Instability**:
   - Check host network connectivity
   - Ensure Docker network is stable
   - Consider using `network_mode: host` if on same machine as HA

### Issue: Health Check Failures

**Symptoms:**
```
Health check failed
Container restarting due to unhealthy status
```

**Diagnosis:**
```bash
docker inspect ha-whatsapp-bridge | grep -A 10 Health
```

**Solutions:**

1. Increase health check timeout:
   ```yaml
   healthcheck:
     timeout: 15s  # Increase from 10s
     interval: 90s  # Check less frequently
   ```

2. Increase `start_period` for slower systems:
   ```yaml
   healthcheck:
     start_period: 60s  # Increase from 30s
   ```

### Issue: Slow Reconnection

**Symptoms:**
- Takes several minutes to reconnect after disconnect
- Exponential backoff reaching maximum delay

**Diagnosis:**

Check HA logs for consecutive failure count:
```
Reconnecting in 300 seconds... (consecutive failures: 8, total reconnects: 15)
```

**Solutions:**

1. **If bridge is healthy but HA takes long to reconnect**:
   - Restart Home Assistant to reset backoff
   - Check HA system resources (CPU, memory)

2. **Adjust backoff parameters** in `client.py` (requires code change):
   ```python
   MIN_RETRY_DELAY = 5  # Start delay (seconds)
   MAX_RETRY_DELAY = 60  # Max delay (reduced from 300)
   ```

### Issue: Bridge Process Crashes

**Symptoms:**
- Container restarts frequently
- Exit code 1 in logs
- Memory or authentication failures

**Diagnosis:**

1. Check container restart count:
   ```bash
   docker ps -a | grep ha-whatsapp-bridge
   ```

2. Check exit reason in logs:
   ```bash
   docker logs ha-whatsapp-bridge 2>&1 | grep -i "exit\|crash\|error"
   ```

**Solutions:**

1. **Authentication Failures**:
   - Clear auth data and re-authenticate:
     ```bash
     docker-compose down
     rm -rf wa-bridge-baileys/.baileys_data/*
     # OR
     rm -rf wa-bridge/.wwebjs_auth/*
     docker-compose up -d
     ```

2. **Memory Crashes**:
   - Increase memory limits as described above
   - Switch to Baileys bridge (uses ~70% less memory)

3. **Chromium Crashes** (whatsapp-web.js only):
   - Add more shared memory:
     ```yaml
     shm_size: 1gb
     ```

## Monitoring Best Practices

### 1. Enable Appropriate Log Levels

For troubleshooting, use FULL logging:
```yaml
environment:
  - INCOMING_MESSAGE_LOG_LEVEL=FULL
```

For production, use COMPACT or NONE:
```yaml
environment:
  - INCOMING_MESSAGE_LOG_LEVEL=COMPACT
```

### 2. Monitor Container Health

Set up monitoring with Portainer, Grafana, or Home Assistant:

```yaml
# Example: Home Assistant automation to alert on bridge health
automation:
  - alias: WhatsApp Bridge Health Alert
    trigger:
      - platform: state
        entity_id: sensor.whatsapp_bridge_status
        to: 'unhealthy'
    action:
      - service: notify.persistent_notification
        data:
          message: "WhatsApp Bridge is unhealthy"
```

### 3. Regular Health Checks

Create a script to monitor health:

```bash
#!/bin/bash
# check-bridge-health.sh

CONTAINER="ha-whatsapp-bridge"

# Check if container is running
if ! docker ps | grep -q $CONTAINER; then
    echo "ERROR: Container not running"
    exit 1
fi

# Check memory usage
MEM_USAGE=$(docker stats --no-stream $CONTAINER --format "{{.MemPerc}}" | sed 's/%//')
if (( $(echo "$MEM_USAGE > 80" | bc -l) )); then
    echo "WARNING: Memory usage high: ${MEM_USAGE}%"
fi

# Check uptime
UPTIME=$(docker inspect $CONTAINER --format='{{.State.StartedAt}}')
echo "Container started: $UPTIME"

echo "Bridge health check passed"
```

## Performance Optimization

### For Memory-Constrained Systems

Use the Baileys bridge instead of whatsapp-web.js:

1. In `docker-compose.yml`, comment out `wa-bridge` and uncomment `wa-bridge-baileys`
2. Restart: `docker-compose up -d`

Memory usage comparison:
- whatsapp-web.js: ~800MB-1.5GB (includes Chromium)
- Baileys: ~200MB-400MB (no browser)

### For High Message Volume

1. Disable incoming messages if not needed:
   ```yaml
   - INCOMING_MESSAGES_MODE=disabled
   ```

2. Use COMPACT or NONE logging:
   ```yaml
   - INCOMING_MESSAGE_LOG_LEVEL=COMPACT
   ```

3. Filter to specific groups/numbers:
   ```yaml
   - ALLOWED_GROUPS=Important Group
   - ALLOWED_NUMBERS=1234567890
   ```

## Getting Help

If issues persist after following this guide:

1. Collect diagnostic information:
   ```bash
   # Container status
   docker ps -a | grep ha-whatsapp-bridge

   # Recent logs
   docker logs ha-whatsapp-bridge --tail 200 > bridge-logs.txt

   # Container stats
   docker stats ha-whatsapp-bridge --no-stream

   # Health check status
   docker inspect ha-whatsapp-bridge | grep -A 20 Health
   ```

2. Check Home Assistant logs for integration errors

3. Open a GitHub issue with:
   - Diagnostic information from above
   - Docker Compose configuration (remove sensitive data)
   - Description of the issue pattern
   - What troubleshooting steps you've tried
