from __future__ import absolute_import

import logging

import requests

log = logging.getLogger(__name__)

READ_ONLY_METHODS = ["GET"]


class CkanApiException(Exception):
    def __init__(self, message, response):
        super(CkanApiException, self).__init__(message)
        self.response = response


class CkanApiFailureException(CkanApiException):
    """
    CKAN API reported {success: false}. It should be okay to continue using the API.
    """

    pass


class CkanApiStatusException(CkanApiException):
    """
    CKAN API returned an unhealthy status code. This indicates something might
    not be working correctly with our configuration or the server could be
    having issues and we should not continue using the API in this state.
    """

    pass


class CkanApiCountException(CkanApiException):
    """
    CKAN API (and solr) returned a non-zero count, but no data. Could this be
    solr index corruption?
    """

    pass


class DryRunException(Exception):
    """
    Something happened during a dry-run execution that shouldn't have, like
    trying to write to the API.
    """

    pass


class CkanApiClient(object):
    """
    Represents a client to query and submit requests to the CKAN API.
    """

    def __init__(
        self,
        api_url,
        api_key,
        dry_run=True,
        identifier_type="identifier",
        api_read_url=None,
        reverse=False,
    ):
        self.api_url = api_url
        if api_read_url is None:
            self.api_read_url = api_url
        else:
            self.api_read_url = api_read_url
        self.dry_run = dry_run
        self.reverse = reverse
        self.client = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.client.mount("https://", adapter)
        self.client.headers.update(Authorization=api_key)
        # Set the auth_tkt cookie to talk to admin API
        self.client.cookies = requests.cookies.cookiejar_from_dict(dict(auth_tkt="1"))
        self.identifier_type = identifier_type

    def request(self, method, path, **kwargs):
        if method == "POST":
            url = "%s/api%s" % (self.api_url, path)
        else:
            url = "%s/api%s" % (self.api_read_url, path)

        if self.dry_run and method not in READ_ONLY_METHODS:
            raise DryRunException("Cannot call method in dry_run method=%s" % method)

        # Set a 60 second timeout for connections
        kwargs.setdefault("timeout", 60)

        response = self.client.request(method, url, **kwargs)
        if response.status_code >= 400:
            log.error(
                "Unsuccessful status code status=%d body=%s",
                response.status_code,
                response.content,
            )
            raise CkanApiStatusException(
                "Unsuccessful status code %d" % response.status_code, response
            )

        if not response.json().get("success", False):
            log.error(
                "API failure status=%d body=%s", response.status_code, response.content
            )
            raise CkanApiFailureException("API reported failure", response)

        return response

    def get(self, path, **kwargs):
        return self.request("GET", path, **kwargs)

    def get_dataset(
        self, organization_name, identifier, is_collection, sort_order="asc"
    ):
        filter_query = '%s:"%s" AND organization:"%s" AND type:dataset' % (
            self.identifier_type,
            identifier,
            organization_name,
        )
        if is_collection:
            filter_query = "%s AND collection_package_id:*" % filter_query

        rows = 1
        response = self.get(
            "/action/package_search",
            params={
                "fq": filter_query,
                "sort": "metadata_modified " + sort_order,
                "rows": rows,
            },
        )

        results = response.json()["result"]["results"]

        if len(results) != rows:
            count = response.json()["result"]["count"]
            raise CkanApiCountException(
                "Query reported non-zero count but no data "
                "count=%(count)s results=%(results)s"
                % {
                    "count": count,
                    "results": len(results),
                },
                response,
            )

        return results[0]

    def check_dataset(self, name):
        response = self.get(
            "/action/package_show",
            params={
                "id": name,
            },
        )
        return response.json()["result"]

    def get_duplicate_identifiers(
        self, organization_name, is_collection, full_count=False
    ):
        filter_query = 'organization:"%s" AND type:dataset' % organization_name
        if is_collection:
            filter_query = "%s AND collection_package_id:*" % filter_query

        response = self.get(
            "/3/action/package_search",
            params={
                "fq": filter_query,
                "facet.field": '["' + self.identifier_type + '"]',
                "facet.limit": -1,
                "facet.mincount": 2,
                "rows": 0,
            },
        )

        dupes = response.json()["result"]["facets"][self.identifier_type]

        # If we want not just the identifiers, but also the counts
        if full_count:
            return dupes

        # If you want to run 2 scripts in parallel, run one version with normal sort
        # and another with `--reverse` flag
        return sorted(dupes, reverse=self.reverse)

    def get_duplicate_identifiers_source(
        self, harvest_source_title, is_collection, full_count=False
    ):
        filter_query = (
            'harvest_source_title:"%s" AND type:dataset' % harvest_source_title
        )
        if is_collection:
            filter_query = "%s AND collection_package_id:*" % filter_query

        response = self.get(
            "/3/action/package_search",
            params={
                "fq": filter_query,
                "facet.field": '["' + self.identifier_type + '"]',
                "facet.limit": -1,
                "facet.mincount": 2,
                "rows": 0,
            },
        )

        dupes = response.json()["result"]["facets"][self.identifier_type]

        # If we want not just the identifiers, but also the counts
        if full_count:
            return dupes

    def get_dataset_count(self, organization_name, identifier, is_collection):
        filter_query = '%s:"%s" AND organization:"%s" AND type:dataset' % (
            self.identifier_type,
            identifier,
            organization_name,
        )
        if is_collection:
            filter_query = "%s AND collection_package_id:*" % filter_query

        response = self.get(
            "/action/package_search",
            params={
                "fq": filter_query,
                "sort": "metadata_created desc",
                "rows": 0,
            },
        )

        return response.json()["result"]["count"]

    def get_datasets_in_collection(self, package_id):
        filter_query = "collection_package_id:%s" % package_id

        response = self.get(
            "/action/package_search",
            params={
                "fq": filter_query,
                "sort": "metadata_created desc",
                "rows": 0,
            },
        )

        search_result = response.json()["result"]
        if search_result["count"] > 0:
            return search_result["results"]
        return None

    def get_datasets(
        self, organization_name, identifier, start=0, rows=1000, is_collection=False
    ):
        filter_query = '%s:"%s" AND organization:"%s" AND type:dataset' % (
            self.identifier_type,
            identifier,
            organization_name,
        )
        if is_collection:
            filter_query = "%s AND collection_package_id:*" % filter_query

        response = self.get(
            "/action/package_search",
            params={
                "fq": filter_query,
                "start": start,
                "rows": rows,
            },
        )

        return response.json()["result"]["results"]

    def get_all_datasets(
        self, start=0, rows=1000, organization="*", is_collection=False
    ):
        filter_query = f"type:dataset AND organization:{organization}"

        response = self.get(
            "/action/package_search",
            params={
                "fq": filter_query,
                "start": start,
                "rows": rows,
            },
        )

        return response.json()["result"]["results"]

    def get_organizations(self):
        response = self.get("/action/organization_list")
        return response.json()["result"]

    def get_harvest_sources(self):
        start = 0
        harvest_sources = []
        while True:
            response = self.get(
                f"/action/package_search?fq=dataset_type:harvest&rows=1000&start={start}"
            )
            harvest_sources += response.json()["result"]["results"]
            if response.json()["result"]["count"] <= start + 1000:
                break
            else:
                start += 1000

        return harvest_sources

    def get_organization_count(self, organization_name):
        response = self.get(
            "/action/package_search?q=organization:%s&rows=0" % organization_name
        )
        return response.json()["result"]["count"]

    def get_harvest_source_count(self, harvest_source_title):
        response = self.get(
            '/action/package_search?q=harvest_source_title:"%s"&rows=0'
            % harvest_source_title
        )
        return response.json()["result"]["count"]

    def remove_package(self, package_id):
        if self.dry_run:
            log.info("Not removing package in dry_run package=%s", package_id)
            return

        self.request(
            "POST",
            "/action/dataset_purge",
            json={
                "id": package_id,
            },
        )

    def update_package(self, package):
        if self.dry_run:
            log.info("Not updating package in dry_run package=%s", package["id"])
            return

        self.request("POST", "/action/package_update", json=package)
