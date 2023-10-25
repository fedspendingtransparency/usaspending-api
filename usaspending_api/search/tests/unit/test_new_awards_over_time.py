from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.search.v2.views.new_awards_over_time import NewAwardsOverTimeVisualizationViewSet


def catch_filter_errors(viewset, filters, expected_exception):
    try:
        viewset.validate_api_request(filters)
    except UnprocessableEntityException:
        if expected_exception == "UnprocessableEntityException":
            assert True
        else:
            assert False, "UnprocessableEntityException error unexpected"
    except InvalidParameterException:
        if expected_exception == "InvalidParameterException":
            assert True
        else:
            assert False, "InvalidParameterException error unexpected"
    except Exception as e:
        print(e)
        assert False, "Incorrect Exception raised"
    else:
        assert False, "Filters should have produced an exception and didn't"


def test_new_awards_filter_errors():
    new_awards_viewset = NewAwardsOverTimeVisualizationViewSet()
    filters = {"group": "baseball"}
    catch_filter_errors(new_awards_viewset, filters, "UnprocessableEntityException")
    filters = {"group": "baseball", "filters": {"recipient_id": "", "time_period": []}}
    catch_filter_errors(new_awards_viewset, filters, "InvalidParameterException")
    filters = {"group": "month", "filters": {"recipient_id": "", "time_period": []}}
    catch_filter_errors(new_awards_viewset, filters, "UnprocessableEntityException")
    filters = {"group": "month", "filters": {"recipient_id": "", "time_period": [{"start_date": ""}]}}
    catch_filter_errors(new_awards_viewset, filters, "UnprocessableEntityException")
