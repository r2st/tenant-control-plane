import logging
import os

import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
# Host execution defaults to loopback. The container sets API_HOST for port forwarding.
uvicorn.run(
    "control_plane.api:create_app", factory=True, host=os.getenv("API_HOST", "127.0.0.1"), port=8000
)
