from __future__ import absolute_import

import argparse
import csv
import itertools
import logging
import logging.config
import os
import sys
import time
from datetime import datetime

from dedupe.ckan_api import CkanApiClient


class OrgDuplicateLog(object):
    # Order matters here for the report
    fieldnames = [
        "name",  # Organization Name
        "number_datasets_duplicated",  # Duplicate identifiers (CKAN ID)
        "total_duplicate_count",  # Sum of all duplicates
        "total_datasets",  # Number of datasets in org
        "percent_duplicate",  # Amount of org datasets are dupes
    ]

    def __init__(self, filename=None, run_id=None):
        if not run_id:
            run_id = datetime.now().strftime("%Y%m%d%H%M%S")

        if not filename:
            filename = "org-duplicates-%s.csv" % run_id

        log.info("Opening duplicate package report for writing filename=%s", filename)
        self.__f = open(filename, mode="w")
        self.log = csv.DictWriter(self.__f, fieldnames=OrgDuplicateLog.fieldnames)
        self.log.writeheader()

    def add(self, org_results):
        log.debug(
            "Recording organization counts=%s", org_results.get("organization_name")
        )
        self.log.writerow(
            {
                "name": org_results.get("name"),
                "number_datasets_duplicated": org_results.get(
                    "number_datasets_duplicated"
                ),
                "total_duplicate_count": org_results.get("total_duplicate_count"),
                "total_datasets": org_results.get("total_datasets"),
                "percent_duplicate": org_results.get("percent_duplicate"),
            }
        )

        # Persist the write to disk
        self.__f.flush()
        os.fsync(self.__f.fileno())


class HarvestDuplicateLog(object):
    # Order matters here for the report
    fieldnames = [
        "harvest_name",  # Harvest Source Name
        "org_name",  # Organization Name
        "number_datasets_duplicated",  # Duplicate identifiers (CKAN ID)
        "total_duplicate_count",  # Sum of all duplicates
        "total_datasets",  # Number of datasets in org
        "percent_duplicate",  # Amount of org datasets are dupes
    ]

    def __init__(self, filename=None, run_id=None):
        if not run_id:
            run_id = datetime.now().strftime("%Y%m%d%H%M%S")

        if not filename:
            filename = "harvest-duplicates-%s.csv" % run_id

        log.info("Opening duplicate package report for writing filename=%s", filename)
        self.__f = open(filename, mode="w")
        self.log = csv.DictWriter(self.__f, fieldnames=HarvestDuplicateLog.fieldnames)
        self.log.writeheader()

    def add(self, harvest_results):
        log.debug(
            "Recording organization counts=%s", harvest_results.get("organization_name")
        )
        self.log.writerow(
            {
                "harvest_name": harvest_results.get("harvest_name"),
                "org_name": harvest_results.get("org_name"),
                "number_datasets_duplicated": harvest_results.get(
                    "number_datasets_duplicated"
                ),
                "total_duplicate_count": harvest_results.get("total_duplicate_count"),
                "total_datasets": harvest_results.get("total_datasets"),
                "percent_duplicate": harvest_results.get("percent_duplicate"),
            }
        )

        # Persist the write to disk
        self.__f.flush()
        os.fsync(self.__f.fileno())


logging.basicConfig(
    stream=sys.stdout, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
log = logging.getLogger("dedupe")
log.setLevel(logging.INFO)

# Define module-level context for signal handling
stopped = False
deduper = None


def get_org_list(ckan):
    log.debug("Fetching organizations...")
    organizations_list = ckan.get_organizations()

    log.debug("Found organizations count=%d", len(organizations_list))
    return organizations_list


def get_harvest_sources(ckan):
    log.debug("Fetching organizations...")
    harvest_sources_list = ckan.get_harvest_sources()

    log.debug("Found organizations count=%d", len(harvest_sources_list))
    return harvest_sources_list


def cleanup(signum, frame):
    global deduper, stopped
    log.warning("Stopping any in-progress dedupers...")
    stopped = True
    deduper.stop()


def run():
    """
    This code for getting the list of organizations and duplicate duplicate data sets
    """
    global deduper, stopped

    parser = argparse.ArgumentParser(
        description="Detects and removes duplicate packages on "
        "data.gov. By default, duplicates are detected but not "
        "actually removed."
    )
    parser.add_argument(
        "--api-url",
        default="https://catalog.data.gov",
        help="The API base URL to query",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Include debug output from urllib3."
    )
    parser.add_argument(
        "--run-id",
        default=datetime.now().strftime("%Y%m%d%H%M%S"),
        help="An identifier for a single run of the deduplication script.",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Include verbose log output."
    )
    parser.add_argument(
        "organization_name",
        nargs="*",
        help="Names of the organizations to deduplicate.",
    )
    parser.add_argument(
        "--harvest_sources", action="store_true", help="Get counts by harvest source"
    )

    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    if args.debug:
        logging.getLogger("urllib3").setLevel(logging.DEBUG)

    log.info("run_id=%s", args.run_id)

    ckan_api = CkanApiClient(args.api_url, "None", identifier_type="identifier")
    ckan_geo_api = CkanApiClient(args.api_url, "None", identifier_type="guid")

    log.info("Using api=%s", args.api_url)

    if args.harvest_sources:
        # Get and organize by harvest source
        harvest_log = HarvestDuplicateLog(run_id=args.run_id)
        harvest_sources = get_harvest_sources(ckan_api)

        log.info("Checking %d harvest sources for duplicates", len(harvest_sources))

        # Loop over the harvest sources one at a time
        count = itertools.count(start=1)
        for s in harvest_sources:
            if stopped:
                break
            log.info("Checking harvest source=%s", s["title"])
            total = ckan_api.get_harvest_source_count(s["title"])

            json_duplicates = ckan_api.get_duplicate_identifiers_source(
                s["title"], False, full_count=True
            )
            geo_duplicates = ckan_geo_api.get_duplicate_identifiers_source(
                s["title"], False, full_count=True
            )
            duplicates = {**json_duplicates, **geo_duplicates}
            count = 0

            for dupe_cnt in duplicates.values():
                count += dupe_cnt - 1

            harvest_source_overview = {
                "harvest_name": s["title"],
                "org_name": s["organization"]["title"],
                "number_datasets_duplicated": len(duplicates),
                "total_duplicate_count": count,
                "total_datasets": total,
                "percent_duplicate": round(
                    float(count) / (total if total > 0 else 1) * 100, 2
                ),
            }

            harvest_log.add(harvest_source_overview)
            time.sleep(1)

    else:
        # Get and organize by org
        org_log = OrgDuplicateLog(run_id=args.run_id)

        if args.organization_name:
            org_list = args.organization_name
        else:
            # get all organizations that have datajson harvester
            org_list = get_org_list(ckan_api)

        log.info("Checking %d organizations for duplicates", len(org_list))

        # Loop over the organizations one at a time
        count = itertools.count(start=1)
        for organization in org_list:
            if stopped:
                break
            log.info("Checking org=%s", organization)
            total = ckan_api.get_organization_count(organization)

            json_duplicates = ckan_api.get_duplicate_identifiers(
                organization, False, full_count=True
            )
            geo_duplicates = ckan_geo_api.get_duplicate_identifiers(
                organization, False, full_count=True
            )
            # duplicates = json_duplicates.copy()
            # duplicates.update(geo_duplicates)
            duplicates = {**json_duplicates, **geo_duplicates}
            count = 0

            for dupe_cnt in duplicates.values():
                count += dupe_cnt - 1

            org_overview = {
                "title": "",
                "name": organization,
                "number_datasets_duplicated": len(duplicates),
                "total_duplicate_count": count,
                "total_datasets": total,
                "percent_duplicate": round(
                    float(count) / (total if total > 0 else 1) * 100, 2
                ),
            }

            org_log.add(org_overview)


if __name__ == "__main__":
    run()
