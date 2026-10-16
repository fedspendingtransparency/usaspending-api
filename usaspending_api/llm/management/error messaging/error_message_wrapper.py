import logging
from functools import cached_property
from typing import Any, Generator

from usaspending_api.llm.models.db_models import AIModel, Message, Session, ToolUse
from usaspending_api.llm.models.py_models import AITool

#ToDO: Add a way for the handler to catch an error where the tool bombs out entirely and does not return a message - currently I think the failure is happening further up the stack and doesn't send a message for the wrapper to handle. Example: Search for /// in the query text.

logger = logging.getLogger(__name__)

def search_error_handler(result, request) -> Callable:
    """A Wrapper that handles exceptions raised by the llm endpoint while executing filter search, injecting custom error messages as approved by the front-end team to have proper messaging to end-users."""


    #Parse the input string from the error, return the appropriate error message
    detailed_message = classify_error(result)

    #Parse the tool input to extract details from the message
    tool_extract = extract_input(request)

    return (detailed_message + tool_extract)
    


def classify_error(basic_error: str) -> Dict[str, Any]:
    """This function is used to sort the incoming message and classify the error into one of the expected states. If no state match is found but an error is encountered a default message is displayed - classified as "BaseResponse"."""

    # handle blank or empty errors up front
    if not basic_error or not isinstance(basic_error, str):
        return {BaseResponse}

    
    #match error message as lowercase to prevent issues
    normalized_basic_error = basic_error.lower()

    classification_match_table = [
        {
            "keywords": ["api timeout", "api took too long"],
            "message": ApiTimeoutErrorMessage
        },
        {
            "keywords": ["database connection failed", "database connection refused", "unable to connect to the database"],
            "message": DbConnectionErrorMessage
        },
        {
            "keywords": ["usage limit"],
            "message": UsageLimitErrorMessage
        },
        {
            "keywords": ["internal connection"],
            "message": InternalConnectionErrorMessage
        },
        {
            "keywords": ["resource limit"],
            "message": ResourceLimitErrorMessage
        },
        {
            "keywords": ["resource unavailable"],
            "message": ResourceUnavailableErrorMessage
        },
        {
            "keywords": ["dns"],
            "message": DnsErrorMessage
        },
        {
            "keywords": ["award id"],
            "message": InvalidAwardIDErrorMessage
        },
        {
            "keywords": ["agency name"],
            "message": InvalidAgencyNameErrorMessage
        },
        {
            "keywords": ["location not found", "location"],
            "message": InvalidLocationErrorMessage
        },
        {
            "keywords": ["recipient id", "recipient"],
            "message": InvalidRecipientIDErrorMessage
        },
        {
            "keywords": ["assistance listing code", "alc", "assistance code"],
            "message": InvalidAssistanceListingCodeErrorMessage
        },
        {
            "keywords": ["naics code", "industry code", "industry classification"],
            "message": InvalidNAICSCodeErrorMessage
        },
        {
            "keywords": ["psc code", "product and service",],
            "message": InvalidPSCCodeErrorMessage
        },
        {
            "keywords": ["defc code", "description code"],
            "message": InvalidDEFCCodeErrorMessage
        },
        {
            "keywords": ["tas account", "gtas account", "treasury accounts system"],
            "message": InvalidTASErrorMessage
        },
        {
            "keywords": ["time format", "date format", "datetime", "invalid date", "invalid time"],
            "message": UnrecognizedTimeFormatErrorMessage
        },
        {
            "keywords": ["unable to filter", "criteria"],
            "message": GeneralFilterErrorMessage
        },
        {
            "keywords": ["filter to awards", "award type"],
            "message": FilterAndAwardTypeMismatchErrorMessage
        },
        {
            "keywords": ["invalid awardee"],
            "message": InvalidAwardeeErrorMessage
        },
        {
            "keywords": ["invalid subawardee", "subawardee"],
            "message": InvalidSubAwardeeErrorMessage
        },
        {
            "keywords": ["no subawardee under this awardee"],
            "message": AwardeeandSubAwardeeMismatchErrorMessage
        },
        {
            "keywords": ["awarding agency"],
            "message": InvalidAwardingAgencyErrorMessage
        },
        {
            "keywords": ["invalid agency", "agency name"],
            "message": InvalidAgencyErrorMessage
        },
        {
            "keywords": ["invalid subagency", "subagency name"],
            "message": InvalidSubAgencyErrorMessage
        },
        {
            "keywords": ["start time format", "start date format", "start datetime", "invalid start date", "invalid start time"],
            "message": InvalidFilterStartDateErrorMessage,
        },
        {
            "keywords": ["end time format", "end date format", "end datetime", "invalid end date", "invalid end time"],
            "message": InvalidFilterEndDateErrorMessage,
        }
        
    ]

    # Scan through possible error states sequentially 
    for state in classification_match_table:
        for wordmatch in state["keywords"]:
            if wordmatch in normalized_msg:
                return {
                    state["message"]
                }
                
    # Fallback to base error message if no other match found
    return {BaseResponse}

def extract_input(tool_input):

    #Try to extract numeric value for specific error
     out_value =list(map(int, re.findall(r'\d+', tool_input)))[0]
     #Default value output whole message 
     if not out_value or not isinstance(out_value, int):
        out_value = tool_input
     return out_value

""" Error message definitions"""

BaseResponse = "We are unable to provide an answer to your question at this time. "

ApiTimeoutErrorMessage = "The API took too long to respond"
DbConnectionErrorMessage = "Unable to connect to the database"
UsageLimitErrorMessage = "The user has reached their usage limit for the day"
InternalConnectionErrorMessage = "Internal connection issue"
ResourceLimitErrorMessage = "Resource Limit has been reached"
ResourceUnavailableErrorMessage = "The requested resource is unavailable"
DnsErrorMessage = "DNS ErrorMessage"

InvalidAwardIDErrorMessage = "Unable to filter. Invalid award ID format: " 
InvalidAgencyNameErrorMessage = "Unable to filter. We couldn’t find an agency with the specified name: "
InvalidLocationErrorMessage = "Unable to filter. Location not recognized: " 
InvalidRecipientIDErrorMessage = "Invalid recipient identifier entered: " 
InvalidAssistanceListingCodeErrorMessage = "Assistance listing code not found: "  
InvalidNAICSCodeErrorMessage = "NAICS code not found: "
InvalidPSCCodeErrorMessage = "PSC code not found: "
InvalidDEFCCodeErrorMessage = "DEFC code not found: "
InvalidTASErrorMessage = "TAS not found: "
UnrecognizedTimeFormatErrorMessage = "Unable to filter based on the specified time period. Please specify a date range in the format dd/mm/yyyy."
GeneralFilterErrorMessage = "Unable to filter awards based on the criteria "
FilterAndAwardTypeMismatchErrorMessage = "Unable to apply the specified filter to awards of this type." 
InvalidAwardeeErrorMessage = "There is no awardee with the specified name "
InvalidSubAwardeeErrorMessage = "There is no sub-awardee with the name "
AwardeeandSubAwardeeMismatchErrorMessage = "There is no sub-awardee under <name of primary awardee> with the name "
InvalidAwardingAgencyErrorMessage = "There is no awarding agency with the name "
InvalidAgencyErrorMessage = "There is no awarding sub-agency with the name "
InvalidSubAgencyErrorMessage =  "There is no awarding sub-agency named <information specified by user> at the agency "
InvalidFilterStartDateErrorMessage = "The filter start date entered is not valid. Please specify a valid date in the format dd/mm/yyyy."
InvalidFilterEndDateErrorMessage = "The filter end date entered is not valid. Please specify a valid date in the format dd/mm/yyyy."