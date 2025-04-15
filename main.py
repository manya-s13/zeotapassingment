import logging
import sys

# Configure logging for the entire application
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Set logging levels for noisy libraries
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Create a logger for this module
logger = logging.getLogger(__name__)
logger.debug("Application starting with enhanced logging")

from app import app  # noqa: F401

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
