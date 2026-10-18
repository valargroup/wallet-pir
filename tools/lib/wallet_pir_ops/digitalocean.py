"""A read-only DigitalOcean API client.

It never follows redirects, so the bearer token is only ever sent to the API
origin, and pagination links must stay on that origin.
"""
import json
import re
import urllib.error
import urllib.parse
import urllib.request

LIMIT = 16 * 1024 * 1024


class DigitalOcean:
    API = 'https://api.digitalocean.com'

    def __init__(self, token):
        self.token = token

    def get(self, path):
        url = self.API + path
        request = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + self.token})
        # Do not follow redirects while carrying credentials.
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *_args, **_kwargs):
                return None
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
            data = response.read(LIMIT + 1)
        if len(data) > LIMIT:
            raise ValueError('oversized provider response')
        return json.loads(data)

    def droplets(self, tag=None):
        """Every droplet, or every droplet carrying `tag`, across all pages."""
        result = []
        path = '/v2/droplets?per_page=200'
        if tag is not None:
            if not isinstance(tag, str) or not re.fullmatch('[A-Za-z0-9_:-]{1,255}', tag):
                raise ValueError('invalid droplet tag')
            path += '&tag_name=' + urllib.parse.quote(tag)
        origin = urllib.parse.urlsplit(self.API)
        visited = set()
        while path:
            if path in visited or len(visited) >= 100:
                raise ValueError('invalid provider pagination')
            visited.add(path)
            page = self.get(path)
            result.extend(page['droplets'])
            following = page.get('links', {}).get('pages', {}).get('next')
            path = None
            if following:
                parsed = urllib.parse.urlsplit(following)
                if parsed.scheme != origin.scheme or parsed.netloc != origin.netloc or parsed.path != '/v2/droplets' or parsed.fragment:
                    raise ValueError('unexpected provider pagination origin')
                path = parsed.path + '?' + parsed.query
        if len({d['id'] for d in result}) != len(result):
            raise ValueError('inconsistent provider pagination')
        return result

    def droplet(self, droplet_id):
        """One droplet by id, or None once DigitalOcean reports it gone (404)."""
        if not re.fullmatch('[1-9][0-9]*', str(droplet_id)):
            raise ValueError('invalid droplet id')
        try:
            return self.get('/v2/droplets/' + str(droplet_id))['droplet']
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise
