# Tank Library

Core Tank functionality including controllers, models, and utilities.

## What's Included

- **Controllers**: DataController, AuthController, ChatController, DocsController, etc.
- **Models**: Data models for all Tank entities
- **Utilities**: Common functions, config, helpers
- **Integrations**: Third-party integrations (GupShup, etc.)

## Installation

### From Git (Production/Regular Developers)
```bash
pip install tank-lib @ git+https://github.com/yourorg/tank-lib@v1.0.0
```

### Local Editable (Core Developers)
```bash
pip install -e /path/to/tank-lib
```

## Usage

```python
from tank.app_data.data_controller import DataController
from tank.app_auth.auth_controller import AuthController
from tank.app_chat.chat_controller import ChatController

# Use controllers
dc = DataController()
result = dc.get_document('portfolio', 'org', 'ring', 'idx')
```

## For Developers

This library contains NO Flask routes. Routes are in `tank-api`.

## Version

Current version: 1.0.0

