from django.core.management.base import BaseCommand
from django.core.management import call_command


class Command(BaseCommand):
    help = 'Load LLM fixture data for AI models and prompts'

    def handle(self, *args, **options):
        self.stdout.write('Loading AI models...')
        call_command('loaddata', 'usaspending_api/llm/fixtures/ai_models.yaml')
        
        self.stdout.write('Loading prompts...')
        call_command('loaddata', 'usaspending_api/llm/fixtures/prompts.yaml')
        
        self.stdout.write(self.style.SUCCESS('Successfully loaded LLM fixtures'))