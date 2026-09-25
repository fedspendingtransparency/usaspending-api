import logging
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError, CommandParser

from usaspending_api.llm.evals.exceptions import EvalError
from usaspending_api.llm.evals.registry import get_eval_class, registered_assistant_names
from usaspending_api.llm.evals.reporting import render_json, render_text, write_xlsx

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Run ground-truth evaluations for one registered LLM assistant.

    Example:
        python manage.py run_llm_eval \
            --assistant filter_search \
            --fail-under 0.95
    """

    help = "Run deterministic ground-truth evaluations for a configured LLM assistant."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--assistant",
            required=True,
            choices=registered_assistant_names(),
            help="Registered assistant evaluator to execute.",
        )
        parser.add_argument(
            "--dataset",
            help="Dataset (ground-truth) file name without the .json suffix. Defaults to the evaluator dataset.",
        )
        parser.add_argument(
            "--case",
            action="append",
            dest="case_names",
            help="Run only a named case. May be supplied more than once.",
        )
        parser.add_argument(
            "--tag",
            "--tags",
            action="append",
            dest="tags",
            help="Run only cases containing a supplied tag. May be supplied more than once.",
        )
        parser.add_argument(
            "--include-unapproved", action="store_true", help="Include draft cases whose approved value is false."
        )
        parser.add_argument(
            "--fail-under",
            type=float,
            default=0.9,
            help="Log a warning when aggregate score is below this value from 0.0 to 1.0. (default: 0.9)",
        )
        parser.add_argument(
            "--format",
            choices=("text", "json", "xlsx"),
            default="text",
            help="Command output format.",
        )
        parser.add_argument(
            "--output",
            type=Path,
            help=(
                "Write the report to this file. Required for xlsx; when omitted, "
                "text and JSON reports are output as logging."
            ),
        )

    def handle(self, *args, **options) -> None:
        """
        Resolve the requested evaluator, execute it, print the report, and log a warning when
        the configured score threshold is not met.
        """
        try:
            evaluator_class = get_eval_class(options["assistant"])
            evaluator = evaluator_class(
                dataset_name=options["dataset"],
                selected_case_names=set(options["case_names"] or []),
                include_unapproved=options["include_unapproved"],
                tags=set(options["tags"] or []),
                fail_under=options["fail_under"],
            )
            summary = evaluator.run()
        except (EvalError, ValueError) as exc:
            raise CommandError(str(exc)) from exc

        output_format = options["format"]
        output_path = options["output"]

        if output_format == "xlsx":
            if output_path is None:
                raise CommandError("--output is required when --format=xlsx.")
            try:
                write_xlsx(summary, output_path)
            except EvalError as error:
                raise CommandError(str(error)) from error
            logger.info(f"Evaluation report written to {output_path}")
        else:
            output = render_json(summary) if output_format == "json" else render_text(summary)
            if output_path is None:
                logger.info(output)
            else:
                try:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(output + "\n", encoding="utf-8")
                except OSError as error:
                    raise CommandError(f"Unable to write evaluation report '{output_path}': {error}") from error
                logger.info(f"Evaluation report written to {output_path}")

        if not summary.passed:
            logger.error(f"Evaluation score {summary.score:.2%} is below required threshold {summary.fail_under:.2%}.")
