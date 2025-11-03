from flask import current_app
import importlib
import os
import sys
import gc

class SchdLoader:
    
    def __init__(self, module_path="handlers"):
        self.module_path = module_path
        
  
        
    def convert_module_name_to_class(self,input_string):
        # Step 1: Get the basename (last part of the path) in a cross-platform way
        after_slash = os.path.basename(input_string)
        
        # Step 2: Replace '_' with spaces
        words = after_slash.replace('_', ' ')
        
        # Step 3: Capitalize the first letter of each word
        capitalized_words = words.title()
        
        # Step 4: Remove spaces
        result = capitalized_words.replace(' ', '')
        
        return result
    
        
    def discover_modules(self,module_path):
        """Recursively finds all Python modules inside the modules directory."""
        module_list = []
        print('Discovering modules')
        
        # Use os.path.join for cross-platform path construction
        path = os.path.join('_tools', module_path, 'handlers')
        print(f'Path:{path}')
        
        # Resolve the absolute path first
        base_path = os.path.abspath(path)
        print(f'Searching in: {base_path}')
        
        # Check if the directory exists
        if not os.path.exists(base_path):
            print(f'Directory not found: {base_path}')
            return module_list
        
        for root, _, files in os.walk(base_path):
            for file in files:
                print(f'File:{file}')
                if file.endswith(".py") and file != "__init__.py":
                    print(f'File ok')
                    # Convert file path into a module path (e.g., "social.create_post")
                    module_relative_path = os.path.relpath(root, base_path)  # Use base_path instead
                    module_name = file[:-3]  # Remove .py extension

                    if module_relative_path == ".":
                        full_module_path = module_name  # Top-level module
                    else:
                        # Use os.path.normpath to handle path separators properly
                        normalized_path = os.path.normpath(module_relative_path)
                        # Replace path separators with dots for module notation
                        full_module_path = f"{normalized_path.replace(os.sep, '.')}.{module_name}"

                    module_list.append(full_module_path)
        return module_list
    


    def discover_modules_x(self):
        """Recursively finds all Python modules inside the modules directory."""
        module_list = []
        print('Discovering modules')
        
        # Resolve the absolute path first
        base_path = os.path.abspath(self.module_path)
        
        # Check if the directory exists
        if not os.path.exists(base_path):
            print(f'Directory not found: {base_path}')
            return module_list
        
        for root, _, files in os.walk(base_path):
            
            for file in files:
                print(f'File:{file}')
                if file.endswith(".py") and file != "__init__.py":
                    print(f'File ok')
                    # Convert file path into a module path (e.g., "social.create_post")
                    module_relative_path = os.path.relpath(root, base_path)  # Get relative path from base folder
                    module_name = file[:-3]  # Remove .py extension

                    if module_relative_path == ".":
                        full_module_path = module_name  # Top-level module
                    else:
                        # Use os.path.normpath to handle path separators properly
                        normalized_path = os.path.normpath(module_relative_path)
                        # Replace path separators with dots for module notation
                        full_module_path = f"{normalized_path.replace(os.sep, '.')}.{module_name}"

                    module_list.append(full_module_path)
        return module_list

    def load_code_class(self, module_path, module_name, class_name, *args, **kwargs):
        """
        Dynamically loads a class from an installed Python package and returns an instance.
        
        This function is completely agnostic - it simply tries to import the module using importlib.
        If the package is installed (via pip), it will be found and imported.
        
        Args:
            module_path: Package name (e.g., "enerclave")
            module_name: Handler name (e.g., "geocoding_handler")
            class_name: Class name (e.g., "GeocodingHandler")
            *args, **kwargs: Arguments to pass to the handler constructor
            
        Returns:
            Handler instance or None if loading fails
            
        Example:
            # For a package installed via: pip install -e enerclave-module/
            # This will import: from enerclave.handlers.geocoding_handler import GeocodingHandler
            instance = load_code_class('enerclave', 'geocoding_handler', 'GeocodingHandler')
        """
        current_app.logger.debug(f"Attempting to load: {module_path}.handlers.{module_name}.{class_name}")
        
        try:
            # Construct the full module path
            # Example: "enerclave.handlers.geocoding_handler"
            full_module_path = f"{module_path}.handlers.{module_name}"
            
            print(f'Loading module: {full_module_path}')
            
            # Dynamically import the module
            # This will work if the package is installed (e.g., via pip install)
            module = importlib.import_module(full_module_path)
            
            print(f'Getting class: {class_name}')
            
            # Get the class from the module
            class_ = getattr(module, class_name)
            
            print(f'Class loaded: {class_.__name__}')
            
            # Instantiate the class
            # Check if it needs config (convention: handlers ending in 'onboardings' need config)
            if 'onboarding' in module_name.lower():
                # Pass tank config to handlers that need it
                config = current_app.tank_config if hasattr(current_app, 'tank_config') else {}
                print(f'Creating instance with config')
                instance = class_(config=config)
            else:
                # Most handlers don't need config in __init__
                print(f'Creating instance')
                instance = class_()
            
            print(f'Instance created: {instance.__class__.__name__}')
            current_app.logger.debug(f"Successfully loaded {class_name} from {full_module_path}")
            
            return instance
            
        except ModuleNotFoundError as e:
            # Module not found - package probably not installed
            current_app.logger.error(f"Module '{full_module_path}' not found: {e}")
            current_app.logger.error(f"Make sure the package is installed via pip (e.g., pip install -e {module_path}-module/)")
            return None
            
        except AttributeError as e:
            # Class not found in module
            current_app.logger.error(f"Class '{class_name}' not found in module '{full_module_path}': {e}")
            return None
            
        except TypeError as e:
            # Error instantiating the class
            current_app.logger.error(f"TypeError when instantiating '{class_name}': {e}")
            return None
            
        except Exception as e:
            # Any other error
            current_app.logger.error(f"Unexpected error loading '{class_name}' from '{full_module_path}': {e}")
            import traceback
            current_app.logger.error(traceback.format_exc())
            return None
        
        

    def load_and_run(self, module_name, *args, **kwargs):
        """Loads a module, runs its class method, then unloads it."""
        action = "load_and_run"
        print(f'running: {action}')
        
        try:
     
            class_name = self.convert_module_name_to_class(module_name)
            print(f'Attempting to load class:{class_name}')
            
            # Handle both file paths and dot-notation module names
            if os.sep in module_name or '/' in module_name:
                # It's a file path - normalize and split using os.sep
                normalized_module_name = os.path.normpath(module_name)
                module_parts = normalized_module_name.split(os.sep)
            else:
                # It's already in dot notation - split by dots
                module_parts = module_name.split('.')
            
            payload = kwargs.get('payload')  # Extract payload from kwargs
            
            # Ensure we have at least 2 parts for module_path and module_name
            if len(module_parts) < 2:
                error = f"Module name '{module_name}' must have at least 2 parts (module_path.module_name)"
                return {'success':False,'action':action,'error':error,'output':error,'status':500}
            
            instance = self.load_code_class(module_parts[0], module_parts[1], class_name, *args, **kwargs)
            runtime_loaded_class = True
    
            if not instance:
                error = f"Class '{class_name}' in '{module_name}' could not be loaded."
                return {'success':False,'action':action,'error':error,'output':error,'status':500}
            
            print(f'Class Loaded:{class_name}')
            
            if hasattr(instance, "run"):       
                result = instance.run(payload)  # Pass payload to run
            else:
                error = f"Class '{class_name}' in '{module_name}' has no 'run' method."
                print(error)
                return {'success':False,'action':action,'error':error,'status':500}


            if runtime_loaded_class:
                # Unload module to free memory
                del instance
                if module_name in sys.modules:
                    del sys.modules[module_name]
                gc.collect()
                
                
            
            if 'success' in result and not result['success']:
                
                return {'success':False,'action':action,'output':result,'status':400} 
            
            return {'success':True,'action':action,'output':result,'status':200}
        
        except Exception as e:
            print(f'Error @load_and_run: {str(e)}')
            return {'success':False,'action':action,'input':class_name,'output':f'Error @load_and_run: {str(e)}'}



# Example Usage
if __name__ == "__main__":
    SHL = SchdLoader()

    