import os
import subprocess
import toml
from foundry_dev_tools import FoundryContext

from t3_code.utility.general_purpose import force_list

class FoundryConnection:

    def __init__(self, config_secret_name: str = "foundry_dev_tools.toml", dataset_secret_name: str = "foundry_datasets.toml"):
        print("INFO: Initializing FoundryConnection...", flush=True)
        
        try:
            self.foundry_context = FoundryConnection.get_FoundryContext_with_fresh_config(config_secret_name)
            self.prefix, self.datasets = FoundryConnection.get_prefix_and_datasets(dataset_secret_name)
            
            FoundryConnection.print_fdt_info()  # print info in dev environemnt
            
            print("SUCCESS: FoundryConnection initialized successfully!", flush=True)
        except Exception as e:
            print(f"ERROR: Failed to initialize FoundryConnection: {str(e)}", flush=True)
            raise


    @staticmethod
    def get_FoundryContext_with_fresh_config(config_secret_name: str = "foundry_dev_tools.toml"):
        """ Check and move the foundry_dev_tools.toml secret to the expected location and create FoundryContext """
        path = "/etc/xdg/foundry-dev-tools/config.toml"
        
        print(f"INFO: Loading Foundry configuration from secret: {config_secret_name}", flush=True)
        
        try:
            with open(f"/run/secrets/{config_secret_name}", "r") as secret_file:
                content = secret_file.read()

                # Check for '[Credentials]', 'domain' and 'jwt' within the .toml
                message = ""
                if "[credentials]" not in content.lower():
                    message += "Section '[credentials]' is missing\n"
                if "domain" not in content:
                    message += "Parameter 'domain' is missing\n"
                if "jwt" not in content:
                    message += "Parameter 'jwt' is missing\n"

                if message:  # Throw a detailed error if the format is invalid
                    error_msg = f"- - - - -\nERROR: foundry_dev_tools.toml secret is not matching the expected format:\n\n[credentials]\n\ndomain = \"\"\n\njwt = \"\"\n\nIssues:\n{message}- - - - -"
                    print(error_msg, flush=True)
                    raise ValueError(f"Invalid foundry_dev_tools.toml configuration: {message.strip()}")

                else:
                    print(f"SUCCESS: foundry_dev_tools.toml secret validation passed.", flush=True)
                    print(f"INFO: Writing config to {path}", flush=True)

                    os.makedirs(os.path.dirname(path), exist_ok=True)  # Create directory if it doesn't exist
                    with open(path, "w") as config_file:  # Write content to file
                        config_file.write(content)
                    
                    print(f"SUCCESS: foundry_dev_tools.toml secret was placed in {path}.", flush=True)
            
            print("INFO: Creating FoundryContext...", flush=True)
            return FoundryContext()
            
        except FileNotFoundError:
            error_msg = f"ERROR: Secret file '/run/secrets/{config_secret_name}' not found!"
            print(error_msg, flush=True)
            raise FileNotFoundError(error_msg)
        except Exception as e:
            error_msg = f"ERROR: Failed to load Foundry configuration: {str(e)}"
            print(error_msg, flush=True)
            raise


    @staticmethod
    def get_prefix_and_datasets(dataset_config_name: str = "foundry_datasets.toml") -> dict:
        """ Load dataset configuration from foundry_datasets.toml secret """

        # TODO: check if datasets have '__' in their name as this is used as a separator in the file names

        print(f"INFO: Loading dataset configuration from secret: {dataset_config_name}", flush=True)
        
        try:
            with open(f"/run/secrets/{dataset_config_name}", "r") as secret_file:
                content = secret_file.read()
                file = toml.loads(content)

                datasets = file.get("datasets", {})
                prefix = file.get("prefix", "ri.foundry.main.dataset.")

                print(f"SUCCESS: Loaded {len(datasets)} datasets with prefix: {prefix}", flush=True)
                return prefix, datasets
                
        except FileNotFoundError:
            error_msg = f"ERROR: Dataset config file '/run/secrets/{dataset_config_name}' not found!"
            print(error_msg, flush=True)
            raise FileNotFoundError(error_msg)
        except Exception as e:
            error_msg = f"ERROR: Failed to load dataset configuration: {str(e)}"
            print(error_msg, flush=True)
            raise


    @staticmethod
    def print_fdt_info():
        """ Execute the 'fdt info' command and print the output """
        if os.getenv("PYTHON_ENV", "production") == "development":
            return

        print("INFO: Executing 'fdt info' command...", flush=True)

        result = subprocess.run(["fdt", "info"], capture_output=True, text=True)
        if result.stdout:
            print(result.stdout, flush=True)
        if result.stderr:
            print(result.stderr, flush=True)
        
        print(f"INFO: 'fdt info' command completed with exit code: {result.returncode}", flush=True)
        print("- - - FDT INFO WAS EXECUTED - - -", flush=True)


    def get_valid_rids(self, names: str | list[str]):
        """ Get valid RIDs for the given names """
        names = force_list(names)

        name_rid_pairs = {}
        not_found = []

        # Implement the method body here
        for name in names:
            if name in self.datasets.keys():
                name_rid_pairs[name] = self.datasets[name]
            else:
                not_found.append(name)

        # Create special message for no valid RIDs
        message = ""
        if not name_rid_pairs:
            message += "Not a single valid RIDs found for your given name(s). "

        # Create detailed error message for unknown datasets
        if not_found:
            datasets_str = "', '".join(not_found)
            message += f"Datasets '{datasets_str}' are unknown. Please only request existing datasets."

        return name_rid_pairs, message
