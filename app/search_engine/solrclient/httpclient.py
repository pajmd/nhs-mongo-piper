import requests


class HttpClientException(Exception):
    pass


class HttpClient(object):
    def __init__(self, host, port=None, timeout=5):
        self.port = port
        self.host = 'http://%s:%s' % (host, port) if port else 'http://%s' % host
        self.timeout = timeout
        self.header = {'user-agent': 'nhs/0.0.1'}

    def get(self, url, json_payload):
        r = requests.get(self.full_url(url), headers=self.header, params=json_payload)
        try:
            r.raise_for_status()
            return r
        except requests.HTTPError:
            return r

    def post(self, url, json_payload=None, fields=None, files=None, header=None):
        """
        Post either a json payload or files
        :param url:
        :param json_payload: json object
        :param fields: list of dict, each dict is a field command
        :param files: single file as a string or a list of files. It is best to pass the full path of a  each file.
        :param header:
        :return: an requests.Response() or list of requests.Response()
        """
        if not self.is_one_olny_set(json_payload, fields, files):
            raise HttpClientException("Error POST - can post only one of json_payload fields, or files")

        headers = self.header.copy()
        if header:
            headers.update(header)
        host_url = self.full_url(url)

        if files:
            r = self.post_files(host_url, headers, files)
        elif fields:
            r = self.post_fields(host_url, headers, fields)
        else:
            r = self.post_json(host_url, headers, json_payload)
        try:
            r.raise_for_status()
            return r
        except requests.HTTPError:
            return r

    def full_url(self, url):
        return '%s/%s' % (self.host, url)

    @staticmethod
    def is_one_olny_set(*p):
        more_than_one__or_name_set = sum(map(bool, [*p])) == 1
        return more_than_one__or_name_set

    @staticmethod
    def post_json(url, headers, json_payload):
        return requests.post(url, headers=headers, json=json_payload)

    @staticmethod
    def post_files(url, headers, files):
        if isinstance(files, list):
            with requests.session() as s:
                for file in files:
                    with open(file, 'rb') as f:
                        r = s.post(url, data=f, headers=headers)
                    try:
                        # any other exception like ConnexionException would be thrown
                        r.raise_for_status()
                    except requests.HTTPError:
                        # we fail at the first troublesome file
                        return r
            return r
        else:
            with open(files, 'rb') as f:
                r = requests.post(url, data=f, headers=headers)
            return r

    @staticmethod
    def post_fields(url, headers, fields):
        scmdlist = ['%s:%s' % (k, v) for cmd in fields for k, v in cmd.items()]
        sflist = '{%s}' % ','.join(scmdlist)
        r = requests.post(url, data=sflist, headers=headers)
        return r
