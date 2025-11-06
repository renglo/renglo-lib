# Renglo Library

Core Renglo functionality including controllers, models, and utilities.

## What's Included

- **Controllers**: DataController, AuthController, ChatController, DocsController, etc.
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


## Installation

Refer to the readme here: 
https://github.com/renglo/system/blob/main/README.md

