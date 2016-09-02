from django.test import TestCase, Client

class AwardTests(TestCase):

    #load the test fixture
    fixtures = ['awards']

    def setUp(self):
        self.c = Client()

    def test_award_list(self):
        """
        Ensure the awards endpoint lists the right number of awards
        """
        resp = self.c.get('/api/v1/awards/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.data), 2)
