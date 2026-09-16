class _ServerResources:
    def __init__(self):
        self._custom_code_modules = []
        self._console_output_loader = None
        self._stop_server = False

    def set_custom_code_modules(self, custom_code_modules):
        self._custom_code_modules = custom_code_modules

    @property
    def custom_code_modules(self):
        return self._custom_code_modules

    @custom_code_modules.setter
    def custom_code_modules(self, _):
        raise RuntimeError("Attempting to set read-only property 'custom_code_modules'")


SERVER_RESOURCES = _ServerResources()
