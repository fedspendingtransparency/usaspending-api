import logging

from django.core.management.base import BaseCommand, CommandError, CommandParser

from usaspending_api.llm.evals.exceptions import EvalError
from usaspending_api.llm.evals.registry import get_eval_class, registered_assistant_names
from usaspending_api.llm.evals.reporting import render_json, render_text

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Run ground-truth evaluations for one registered LLM assistant.

    Example:
        python manage.py run_llm_eval \
            --assistant filter_search \
            --fail-under 1.0
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
            help="Fail when aggregate score is below this value from 0.0 to 1.0.",
        )
        parser.add_argument(
            "--allow-extra-tool-arguments",
            action="store_true",
            help=(
                "Treat expected tool-call arguments as a required subset rather than requiring exact argument equality."
            ),
        )
        parser.add_argument(
            "--format",
            choices=("text", "json"),
            default="text",
            help="Command output format.",
        )

    def handle(self, *args, **options) -> None:
        """
        Resolve the requested evaluator, execute it, print the report, and return a non-zero command result when
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
                executor_path=options["executor"],
                allow_extra_tool_arguments=options["allow_extra_tool_arguments"],
            )
            summary = evaluator.run()
        except (EvalError, ValueError) as exc:
            raise CommandError(str(exc)) from exc

        output = render_json(summary) if options["format"] == "json" else render_text(summary)

        logger.info(output)

        if not summary.passed:
            raise CommandError(
                f"Evaluation score {summary.score:.2%} is below required threshold {summary.fail_under:.2%}."
            )
