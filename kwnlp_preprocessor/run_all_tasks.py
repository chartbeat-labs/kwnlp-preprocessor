# Copyright 2021-present Kensho Technologies, LLC.
import logging
from typing import List

from kwnlp_preprocessor import (
    argconfig,
    task_00_download_raw_dumps,
    task_03p1_create_kwnlp_pagecounts,
    task_03p2_convert_sql_to_csv,
    task_06p1_create_kwnlp_page_props,
    task_06p2_create_kwnlp_redirect_it2,
    task_09p1_create_kwnlp_ultimate_redirect,
    task_12p1_create_kwnlp_title_mapper,
    task_15p1_split_and_compress_wikidata,
    task_18p1_filter_wikidata_dump,
    task_21p1_gather_wikidata_chunks,
    task_24p1_create_kwnlp_article_pre,
    task_27p1_parse_wikitext,
    task_30p1_post_process_link_chunks,
    task_33p1_collect_post_processed_link_data,
    task_36p1_collect_template_data,
    task_36p2_collect_length_data,
    task_39p1_create_kwnlp_article,
    task_42p1_collect_section_names,
)

logger = logging.getLogger(__name__)


def main(
    wp_yyyymmdd: str,
    wd_yyyymmdd: str,
    data_path: str = argconfig.DEFAULT_KWNLP_DATA_PATH,
    wiki: str = argconfig.DEFAULT_KWNLP_WIKI,
    mirror_url: str = argconfig.DEFAULT_KWNLP_WIKI_MIRROR_URL,
    jobs_to_download: List[str] = argconfig.DEFAULT_KWNLP_DOWNLOAD_JOBS.split(","),
    max_entities: int = argconfig.DEFAULT_KWNLP_MAX_ENTITIES,
    workers: int = argconfig.DEFAULT_KWNLP_WORKERS,
    include_item_statements: bool = False,
) -> None:
    logger.info("Starting tasks, will download everything to: %s", data_path)
    task_00_download_raw_dumps.main(
        wp_yyyymmdd,
        wd_yyyymmdd,
        data_path=data_path,
        mirror_url=mirror_url,
        wiki=wiki,
        jobs_to_download=jobs_to_download,
    )
    logger.info("Done Downloading")
    logger.info("Starting pagecount creations")
    task_03p1_create_kwnlp_pagecounts.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with pagecounts")
    logger.info("Starting sql to csv")
    task_03p2_convert_sql_to_csv.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with sql to csv")
    logger.info("Starting page props")
    task_06p1_create_kwnlp_page_props.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with page props")
    logger.info("Starting redirect it2")
    task_06p2_create_kwnlp_redirect_it2.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with redirect it2")
    logger.info("Starting ultimite redirect")
    task_09p1_create_kwnlp_ultimate_redirect.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with ultimate redirect")
    logger.info("Starting Title Mapper")
    task_12p1_create_kwnlp_title_mapper.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with Title Mapper")
    logger.info("Starting compression of wikidata")
    task_15p1_split_and_compress_wikidata.main(
        wd_yyyymmdd, data_path=data_path, max_entities=max_entities
    )
    logger.info("Done with compression of wikidata")
    logger.info("Starting wikidata filter")
    task_18p1_filter_wikidata_dump.main(
        wd_yyyymmdd,
        data_path=data_path,
        wiki=wiki,
        workers=workers,
        max_entities=max_entities,
        include_item_statements=include_item_statements,
    )
    logger.info("Done with wikidata filter")
    logger.info("Starting gather wikidata chunks")
    task_21p1_gather_wikidata_chunks.main(wd_yyyymmdd, data_path=data_path)
    logger.info("Done with wikidata chunks")
    logger.info("Starting create article pre")
    task_24p1_create_kwnlp_article_pre.main(
        wp_yyyymmdd, wd_yyyymmdd, data_path=data_path, wiki=wiki
    )
    logger.info("Done with create article pre")
    logger.info("Starting parse wikitext")
    task_27p1_parse_wikitext.main(
        wp_yyyymmdd,
        data_path=data_path,
        wiki=wiki,
        workers=workers,
        max_entities=max_entities,
    )
    logger.info("Done with parse wikitext")
    logger.info("Starting post process linking")
    task_30p1_post_process_link_chunks.main(
        wp_yyyymmdd, data_path=data_path, wiki=wiki, workers=workers
    )
    logger.info("Done with postprocess linking")
    logger.info("Starting collect post processed")
    task_33p1_collect_post_processed_link_data.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with cpp")
    logger.info("Starting collect template")
    task_36p1_collect_template_data.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with collect template")
    logger.info("Starting collect length")
    task_36p2_collect_length_data.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with collect length")
    logger.info("Starting create article")
    task_39p1_create_kwnlp_article.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with create article")
    logger.info("Starting collect section")
    task_42p1_collect_section_names.main(wp_yyyymmdd, data_path=data_path, wiki=wiki)
    logger.info("Done with collect data")

    logger.info("Done with all tasks")
    logger.info("Everything should be saved to: %s", data_path)


if __name__ == "__main__":

    description = "run all wikimedia ingestion tasks"
    arg_names = [
        "wp_yyyymmdd",
        "wd_yyyymmdd",
        "data_path",
        "mirror_url",
        "wiki",
        "jobs",
        "max_entities",
        "workers",
        "loglevel",
    ]
    parser = argconfig.get_argparser(description, arg_names)

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger.info(f"args={args}")
    jobs_to_download = argconfig.list_from_comma_delimited_string(args.jobs)

    main(
        args.wp_yyyymmdd,
        args.wd_yyyymmdd,
        data_path=args.data_path,
        mirror_url=args.mirror_url,
        wiki=args.wiki,
        jobs_to_download=jobs_to_download,
        max_entities=args.max_entities,
        workers=args.workers,
    )
