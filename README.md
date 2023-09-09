# Home-Loan-Approval-System-using-pySpark

Consider one of the leading financial organization that provides Health Insurance, Home loan, Education Loan, and other banking services globally to its customers. 


Approving an online home loan application is critical as it requires proper verification and validation of the user information. To provide optimized user experience, organization has decided to provide instant online home loan approval.


**In the approval process, below business rules to be verified.**

* Customer’s defaulter status
* Customer’s credit score which provides an insight for the eligibility of approving a home loan application

**Project Implementation:**

File Name: HomeLoanApplicationData.csv

This file contains customer application data provided while applying the Home Loan request online.

**Schema:** 

CustomerName:STRING, DOB:STRING, UIN:STRING, MailID:STRING, PhoneNumber:LONG, City:STRING, State:STRING, LivingStatus:STRING, PinCode:STRING, LoanAmount:LONG

**Sample Data:**

Sakshi, 22-03-86, UIN0043, Sakshi@mail.com, 3344990876, Ahmedabad, Gujarat, BPL ,380001, 23000
Shivani, 22-02-83, UIN0044, Shivani@mail.com, 3344990876, Thiruvananthpuram,   Kerala, APL, 695001,24500


File Name:  ClientReferenceData.csv

This file contains the applicant’s reference data to verify the genuine person shall get benefits only. This data is collected from Governmental records directly.

**Schema:**

CustomerName:STRING, DOB:STRING,UIN:STRING, City:STRING, State:STRING, PinCode:LONG, CibilScore:LONG, DefaulterFlag:STRING

**Sample data:**

Shubham, 23-08-86, UIN0007, Thiruvananthpuram, Kerala, 695001, 3530, N
Anushka, 25-08-82, UIN0008, Thiruvananthpuram, Kerala, 695001, 1530, Y



**Implementation Tasks**

Task 1: Create Spark SQL DataFrames

* Create two RDDs applicationDataRDD and ReferenceDataRDD by consuming data from  HomeLoanApplicationData.csv and ClientReferenceData.csv files respectively
* Convert both RDDs into Spark SQL DataFrames

Task 2: Process the client application data

* Write Spark SQL queries to join both the DataFrames and validate the client application data against Client reference data.

**Business rules to implement:**

* Identify the customers with defaulter status.
* Count the customers whose LivingStatus is “BPL” (Below Poverty Line) or “APL.” (Above Poverty Line)
* Apply the status to be Approved if the client is not a defaulter and the credit score is more than 800
* Display the UIN, Client Name, Status (Approved/Rejected), loan amount  and save the details into Hive table.
* Find out the customer's name who are eligible for home loan. Given below the criteria for eligibility:
  * Customer LivingStatus should be “BPL”
  * Customer should not be defaulter
  * Customer Credit score should be more than 800
