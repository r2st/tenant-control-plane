import logging

import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
# Listen on the container interface; Compose publishes the port only on host loopback.
uvicorn.run("control_plane.api:create_app", factory=True, host="0.0.0.0", port=8000)  # nosec B104
