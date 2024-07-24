FORMAT: 1A
HOST: https://api.usaspending.gov

# Award Types [/api/v2/references/award_types/]

This endpoint returns a JSON object representing the grouping of award types.
​
## GET
+ Response 200 (application/json)
    + Attributes (object)
        + `contracts` (required, ContractTypes, fixed-type)
        + `loans` (required, LoanTypes, fixed-type)
        + `idvs` (required, IDVTypes, fixed-type)
        + `grants` (required, GrantTypes, fixed-type)
        + `other_financial_assistance` (required, OtherFinancialAssistanceTypes, fixed-type)
        + `direct_payments` (required, DirectPaymentTypes, fixed-type)
    + Body

            {
                "contracts": {
                    "A": "BPA Call",
                    "B": "Purchase Order",
                    "C": "Delivery Order",
                    "D": "Definitive Contract"
                },
                "loans": {
                    "07": "Direct Loan",
                    "08": "Guaranteed/Insured Loan"
                },
                "idvs": {
                    "IDV_A": "GWAC Government Wide Acquisition Contract",
                    "IDV_B": "IDC Multi-Agency Contract, Other Indefinite Delivery Contract",
                    "IDV_B_A": "IDC Indefinite Delivery Contract / Requirements",
                    "IDV_B_B": "IDC Indefinite Delivery Contract / Indefinite Quantity",
                    "IDV_B_C": "IDC Indefinite Delivery Contract / Definite Quantity",
                    "IDV_C": "FSS Federal Supply Schedule",
                    "IDV_D": "BOA Basic Ordering Agreement",
                    "IDV_E": "BPA Blanket Purchase Agreement"
                },
                "grants": {
                    "02": "Block Grant",
                    "03": "Formula Grant",
                    "04": "Project Grant",
                    "05": "Cooperative Agreement"
                },
                "other_financial_assistance": {
                    "09": "Insurance",
                    "11": "Other Financial Assistance",
                    "-1": "Not Specified"
                },
                "direct_payments": {
                    "06": "Direct Payment for Specified Use",
                    "10": "Direct Payment with Unrestricted Use"
                }
            }

# Data Structures
## ContractTypes (object)
+ `A` (required, string) - BPA Call
+ `B` (required, string) - Purchase Order
+ `C` (required, string) - Delivery Order
+ `D` (required, string) - Definitive Contract
​
## LoanTypes (object)
+ `07` (required, string) - Direct Loan
+ `08` (required, string) - Guaranteed/Insured Loan

## IDVTypes (object)
+ `IDV_A` (required, string) - GWAC Government Wide Acquisition Contract
+ `IDV_B` (required, string) - IDC Multi-Agency Contract, Other Indefinite Delivery Contract
+ `IDV_B_A` (required, string) - IDC Indefinite Delivery Contract / Requirements
+ `IDV_B_B` (required, string) - IDC Indefinite Delivery Contract / Indefinite Quantity
+ `IDV_B_C` (required, string) - IDC Indefinite Delivery Contract / Definite Quantity
+ `IDV_C` (required, string) - FSS Federal Supply Schedule
+ `IDV_D` (required, string) - BOA Basic Ordering Agreement
+ `IDV_E` (required, string) - BPA Blanket Purchase Agreement

## GrantTypes (object)
+ `02` (required, string) - Block Grant
+ `03` (required, string) - Formula Grant
+ `04` (required, string) - Project Grant
+ `05` (required, string) - Cooperative Agreement

## OtherFinancialAssistanceTypes (object)
+ `09` (required, string) - Insurance
+ `11` (required, string) - Other Financial Assistance
​
## DirectPaymentTypes (object)
+ `06` (required, string) - Direct Payment for Specified Use
+ `10` (required, string) - Direct Payment with Unrestricted Use
+ `-1` (required, string) - Not Specified