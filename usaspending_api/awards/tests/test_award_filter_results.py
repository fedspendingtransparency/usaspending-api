import logging
import pytest
import json
from rest_framework import status


@pytest.mark.django_db
def test_false_foreign_recipient_location_award(client):
    """
    Tests that the DUNS code of a US-based recipient while
    filtering for foreign recipients only
    does not show in search results
    """
    logger = logging.getLogger('console')
    resp = client.post('/api/v2/search/spending_by_award_count',
                       content_type='application/json',
                       data=json.dumps(
                           {
                               "filters": {
                                   "time_period": [
                                       {
                                           "start_date": "2012-10-01",
                                           "end_date": "2013-09-30"
                                       },
                                       {
                                           "start_date": "2013-10-01",
                                           "end_date": "2014-09-30"
                                       },
                                       {
                                           "start_date": "2014-10-01",
                                           "end_date": "2015-09-30"
                                       },
                                       {
                                           "start_date": "2015-10-01",
                                           "end_date": "2016-09-30"
                                       },
                                       {
                                           "start_date": "2016-10-01",
                                           "end_date": "2017-09-30"
                                       },
                                       {
                                           "start_date": "2017-10-01",
                                           "end_date": "2018-09-30"
                                       },
                                       {
                                           "start_date": "2007-10-01",
                                           "end_date": "2008-09-30"
                                       },
                                       {
                                           "start_date": "2018-10-01",
                                           "end_date": "2019-09-30"
                                       },
                                       {
                                           "start_date": "2008-10-01",
                                           "end_date": "2009-09-30"
                                       },
                                       {
                                           "start_date": "2009-10-01",
                                           "end_date": "2010-09-30"
                                       },
                                       {
                                           "start_date": "2010-10-01",
                                           "end_date": "2011-09-30"
                                       },
                                       {
                                           "start_date": "2011-10-01",
                                           "end_date": "2012-09-30"
                                       }
                                   ],
                                   "recipient_search_text": [
                                       "877936518"
                                   ],
                                   "recipient_scope": "foreign"
                               }
                           }
                       )
                       )
    assert resp.status_code == status.HTTP_200_OK
    data = json.loads(resp.body)
    logger.info(data)

    results = data["results"]
    for subtype in results.items():
        value = results[subtype]
        assert value == 0


@pytest.mark.django_db
def test_true_foreign_recipient_location_award(client):
    """
    Tests that the DUNS code of a foreign recipient returns true correctly
    """
    logger = logging.getLogger('console')
    resp = client.post('/api/v2/search/spending_by_award_count',
                       content_type='application/json',
                       data=json.dumps(
                           {
                               "filters": {
                                   "time_period": [
                                       {
                                           "start_date": "2012-10-01",
                                           "end_date": "2013-09-30"
                                       },
                                       {
                                           "start_date": "2013-10-01",
                                           "end_date": "2014-09-30"
                                       },
                                       {
                                           "start_date": "2014-10-01",
                                           "end_date": "2015-09-30"
                                       },
                                       {
                                           "start_date": "2015-10-01",
                                           "end_date": "2016-09-30"
                                       },
                                       {
                                           "start_date": "2016-10-01",
                                           "end_date": "2017-09-30"
                                       },
                                       {
                                           "start_date": "2017-10-01",
                                           "end_date": "2018-09-30"
                                       },
                                       {
                                           "start_date": "2007-10-01",
                                           "end_date": "2008-09-30"
                                       },
                                       {
                                           "start_date": "2018-10-01",
                                           "end_date": "2019-09-30"
                                       },
                                       {
                                           "start_date": "2008-10-01",
                                           "end_date": "2009-09-30"
                                       },
                                       {
                                           "start_date": "2009-10-01",
                                           "end_date": "2010-09-30"
                                       },
                                       {
                                           "start_date": "2010-10-01",
                                           "end_date": "2011-09-30"
                                       },
                                       {
                                           "start_date": "2011-10-01",
                                           "end_date": "2012-09-30"
                                       }
                                   ],
                                   "recipient_search_text": [
                                       "877936518"
                                   ],
                                   "recipient_scope": "foreign"
                               }
                           }
                       )
                       )
    assert resp.status_code == status.HTTP_200_OK
    data = json.loads(resp.body)
    logger.info(data)

    results = data["results"]
    assert results['contracts'] > 0
