from django.test import TestCase, Client

class AwardTests(TestCase):

    #load the test fixture
    fixtures = ['awards']


    def test_award_list(self):
        """
        Ensure the awards endpoint lists the right number of awards
        """
        resp = self.client.get('/api/v1/awards/')
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.data), 2)
