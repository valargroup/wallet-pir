"""Bounded read-only GitHub API requests; transport failures never become success."""
import json
import os
import ssl
import subprocess
import time
import urllib.error
import urllib.request


class DropAuthOnRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None:
            redirected.remove_header('Authorization')
        return redirected


def api(path, *, timeout=10, attempts=3):
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    for attempt in range(attempts):
        try:
            if not token:
                return json.loads(subprocess.check_output(['gh', 'api', path], timeout=timeout))
            request = urllib.request.Request(
                os.environ.get('GITHUB_API_URL', 'https://api.github.com') + '/' + path,
                headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
                         'X-GitHub-Api-Version': '2022-11-28'})
            with urllib.request.build_opener(DropAuthOnRedirect).open(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            error.close()
            if error.code not in (429, 500, 502, 503, 504) or attempt + 1 == attempts:
                raise
        except urllib.error.URLError as error:
            if isinstance(error.reason, ssl.SSLCertVerificationError) or attempt + 1 == attempts:
                raise
        except ssl.SSLCertVerificationError:
            raise
        except (TimeoutError, ConnectionError, ssl.SSLError, subprocess.TimeoutExpired):
            if attempt + 1 == attempts:
                raise
        time.sleep(.25 * (attempt + 1))
    raise AssertionError('unreachable retry state')
