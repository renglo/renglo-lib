# Renglo Library

Core Renglo functionality including controllers, models, and utilities.

## What's Included

- **Controllers**: DataController, AuthController, ChatController, FilesController, etc.
- **Models**: Data models for all Renglo entities
- **Utilities**: Common functions, config, helpers
- **Integrations**: Third-party integrations (GupShup, etc.)

## Installation

### From Git (Production/Regular Developers)
```bash
pip install renglo-lib @ git+https://github.com/yourorg/renglo-lib@v1.0.0
```

### Local Editable (Core Developers)
```bash
pip install -e /path/to/renglo-lib
```

## Usage

```python
from renglo.data.data_controller import DataController
from renglo.auth.auth_controller import AuthController
from renglo.chat.chat_controller import ChatController

# Use controllers
dc = DataController()
result = dc.get_document('portfolio', 'org', 'ring', 'idx')
```

## For Developers

This library contains NO Flask routes. Routes are in `renglo-api`.

## White-label (`import wl`)

Invite emails and product name come from the tenant **`<tenant>-wl`** pack
(import name always `wl`). This library does **not** depend on that pack.

- **Local:** install it into the API venv (`pip install -e ../<tenant>-wl` from
  `renglo-api`), then restart the server. See `dev/renglo-api/README.md`.
- **Staging / production:** the backend image installs the BOM `python` pin
  (`<tenant>-wl`). No extra env var. If `wl` is not installed, copy falls back
  to `Renglo` — never `WL_NAME`.

## License

This project is licensed under the MIT License. See [LICENSE.txt](LICENSE.txt) for details.


## Installation

Refer to the local setup guide in `renglo-api` (`dev/renglo-api/README.md`).

