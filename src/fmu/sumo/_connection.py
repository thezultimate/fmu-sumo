from sumo.wrapper import CallSumoApi

class SumoConnection:
    """Object to hold authentication towards Sumo"""

    def __init__(self, env=None):
        self._api = None
        self._env = env

        print('Connection to Sumo on environment: {}'.format(self.env))

    @property
    def env(self):
        if self._env is None:
            self._env = 'dev'
        return self._env

    @property
    def userdata(self):
        return self.api.userdata

    @property
    def api(self):
        if self._api is None:
            self._api = self._establish_connection()
            name = self._api.userdata.get('name')
            upn = self._api.userdata.get('profile').get('userPrincipalName')
            print(f"Authenticated user: {name} ({upn})")

        return self._api

    def refresh(self):
        """Re-create the connection"""
        self._api = self._establish_connection()        

    def _establish_connection(self):
        """Establish the connection with Sumo API, take user through 2FA."""
        api = CallSumoApi(env=self.env)
        api.get_bear_token()
        return api
