#!/usr/bin/env python3
"""
generate_hr_500k_full_schema.py

Generates a single flat HR CSV with 500,000 rows and the *exact* schema required,
including descriptive fields + matching ID columns for each categorical field.
"""

import csv
import random
import datetime as dt
import os
from math import ceil

# ---------- CONFIG ----------
TOTAL_ROWS = 500_000
NUM_EMPLOYEES = 8000
CHUNK_SIZE = 50_000
OUTPUT_CSV = "hr_flat_500k_full_schema.csv"
SEED = 42
# ----------------------------

if SEED is not None:
    random.seed(SEED)

TODAY = dt.date.today()

def random_date(start_year=1970, end_year=2025):
    y = random.randint(start_year, end_year)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return dt.date(y, m, d)

def random_date_between(start_date, end_date):
    start_ord = start_date.toordinal()
    end_ord = end_date.toordinal()
    return dt.date.fromordinal(random.randint(start_ord, end_ord))

def iso(d):
    return d.isoformat() if isinstance(d, dt.date) else str(d)

# ---------- DOMAIN VALUES ----------
DOMAINS = {
    "Gender": ["Male","Female","Other"],
    "Department": ["HR","Finance","Engineering","Sales","Marketing","Operations","Legal","IT","Support","R&D"],
    "JobTitle": ["Software Engineer","Senior Software Engineer","Lead Engineer","Manager","Senior Manager","Director",
                 "HR Executive","HR Manager","Finance Executive","Finance Manager","Sales Executive","Sales Manager",
                 "Marketing Executive","Operations Manager","Support Engineer","Product Manager","Data Analyst",
                 "DevOps Engineer","QA Engineer","Business Analyst"],
    "EmploymentStatus": ["Active","On Leave","Resigned","Terminated","Retired","Probation"],
    "Location": ["Pune","Mumbai","Bengaluru","Chennai","Delhi","Hyderabad","Kolkata","Jaipur","Ahmedabad","Noida"],
    "EducationLevel": ["High School","Diploma","Bachelors","Masters","MBA","PhD","Certification"],
    "MaritalStatus": ["Single","Married","Divorced","Widowed"],
    "Nationality": ["India","United States","United Kingdom","Canada","Australia","Germany","France","Brazil","Japan","South Africa"],
    "CertificationName": ["PMP","AWS Certified","Azure Certified","Scrum Master","Six Sigma","Data Science Certification","CFA","SHRM"],
    "EmploymentType": ["Full-Time","Part-Time","Contractor","Intern"],
    "CertificationStatus": ["Active","Expired","Revoked","In Progress"],
    "TrainingProgram": ["Onboarding","Leadership","Technical Bootcamp","Soft Skills","Compliance","Manager Training"],
    "SkillCategory": ["Technical","Leadership","Communication","Analytical","Creative","Management"],
    "ReviewerRole": ["Manager","Peer","HR","External Consultant"],
    "LearningPath": ["Beginner","Intermediate","Advanced","Expert"],
    "CertificationProvider": ["PMI","Amazon","Microsoft","Scrum.org","ASQ","Coursera","Udemy"],
    "CertificationLevel": ["Level 1","Level 2","Level 3","Master"],
    "ReviewPeriod": ["Quarterly","Bi-Annual","Annual"],
    "PromotionEligibility": ["Eligible","Not Eligible"],
    "ReviewType": ["Self Review","Manager Review","360 Review"],
    "ReviewStatus": ["Pending","Completed","Overdue"],
    "ReviewLocation": ["Onsite","Remote","Hybrid"],
    "CompetencyCategory": ["Technical Skills","Soft Skills","Leadership","Problem Solving"],
    "GoalCategory": ["Business","Personal Development","Innovation"],
    "FeedbackType": ["Positive","Constructive","Neutral"],
    "TrainingRecommendation": ["Yes","No"],
    "ImprovementPlan": ["Yes","No"],
    "InternalPromotion": ["Yes","No"],
    "InitiativeParticipation": ["High","Medium","Low","None"],
    "PerformancePreference": ["Individual","Team"],
    "MentalHealthSupportUsage": ["Yes","No"],
    "CulturalFit": ["High","Medium","Low"],
    "TeamDynamics": ["Cohesive","Fragmented","Collaborative"],
    "Feedback": ["Positive","Negative","Neutral"],
    "EngagementLevel": ["High","Medium","Low"],
    "InternalTransferStatus": ["Transferred","Not Transferred"],
    "Mobility": ["High","Medium","Low"],
    "WorkplaceSupportLevel": ["Excellent","Good","Average","Poor"],
    "PolicyViolations": ["None","Minor","Major"],
    "ComplianceTrainingStatus": ["Completed","Pending","Overdue"],
    "CareerPath": ["Technical","Managerial","Hybrid"],
    "WorkExperience": ["0-2 years","3-5 years","6-10 years","10+ years"],
    "RecruitmentChannel": ["Referral","Job Portal","Campus","Agency","Direct"],
    "Project": ["Project A","Project B","Project C","Project D"],
    "Asset": ["Laptop","Desktop","Tablet","Mobile"],
    "Shift": ["Morning","Evening","Night","Rotational"],
    "LeaveType": ["Sick","Casual","Earned","Unpaid"],
    "LeaveStatus": ["Approved","Pending","Rejected"],
    "AttendanceStatus": ["Present","Absent","Late","Half Day"],
    "Bank": ["HDFC","ICICI","SBI","Axis","Kotak"],
    "TrainerName": ["Trainer A","Trainer B","Trainer C","Trainer D"],
    "TrainingType": ["Online","Offline","Hybrid"],
    "TrainingLocation": ["Pune","Mumbai","Bengaluru","Chennai","Delhi"],
    "ExitReason": ["Personal","Better Opportunity","Retirement","Termination"],
    "DaysWorked": ["<100","100-200","200-300",">300"]
}

# ---------- LOOKUP DICTIONARIES ----------
def make_id_lookup(values, prefix):
    return {val: f"{prefix}{i+1:03d}" for i, val in enumerate(values)}

LOOKUPS = {k: make_id_lookup(v, k[:3].upper()) for k, v in DOMAINS.items()}

# ---------- EMPLOYEE MASTER ----------
FIRST_NAMES = ["Alex","Sam","Jordan","Taylor","Chris","Pat","Jamie","Morgan","Casey","Riley",
               "Amit","Sneha","Priya","Rohan","Anjali","Deepak","Pallavi","Vikram","Neha","Karan"]
LAST_NAMES = ["Sharma","Patel","Kumar","Singh","Iyer","Reddy","Khan","Gupta","Joshi","Mehta",
              "Smith","Johnson","Williams","Brown","Jones","Miller","Davis","Wilson","Moore","Taylor"]

def build_employee_master(eid):
    dob = random_date(1960, 2000)
    hire_date = random_date_between(dob + dt.timedelta(days=18*365), TODAY)
    fname = random.choice(FIRST_NAMES)
    lname = random.choice(LAST_NAMES)
    base_salary = random.randint(20000, 300000)

    return {
        "EmployeeID": f"E{eid:06d}",
        "FirstName": fname,
        "LastName": lname,
        "DateOfBirth": iso(dob),
        "DateOfHire": iso(hire_date),
        "BaseSalary": base_salary
    }

EMPLOYEES = {i: build_employee_master(i) for i in range(1, NUM_EMPLOYEES+1)}
manager_ids = random.sample(list(EMPLOYEES.keys()), k=max(50, NUM_EMPLOYEES // 10))

def choose_manager_for(eid):
    mgr = random.choice(manager_ids)
    if mgr == eid:
        mgr = random.choice([m for m in manager_ids if m != eid])
    return f"E{mgr:06d}", f"MGR{mgr:06d}"

# ---------- COLUMNS ----------
COLUMNS = [
    "EmployeeID","FirstName","LastName","Gender","GenderID","DateOfBirth","DateOfHire",
    "Department","DepartmentID","JobTitle","JobTitleID","EmploymentStatus","EmploymentStatusID",
    "Location","LocationID","EducationLevel","EducationLevelID","MaritalStatus","MaritalStatusID",
    "Nationality","NationalityID","CertificationName","CertificationID","EmploymentType","EmploymentTypeID",
    "BaseSalary","Bonus","TenureMonth","CertificationDate","CertificationStatus","CertificationStatusID",
    "TrainingProgram","TrainingProgramID","TrainingHours","TrainingScore","SkillCategory","SkillCategoryID",
    "ReviewerRole","ReviewerRoleID","SkillAssessmentScore","LearningPath","LearningPathID",
    "CertificationProvider","CertificationProviderID","CertificationLevel","CertificationLevelID",
    "TrainingFeedbackScore","RenewalRequired","RenewalQueDate","ReviewPeriod","ReviewPeriodID",
    "PromotionEligibility","PromotionEligibilityID","ReviewType","ReviewTypeID","ReviewStatus","ReviewStatusID",
    "ReviewLocation","ReviewLocationID","PerformanceScore","CompetencyCategory","CompetencyCategoryID",
    "GoalCategory","GoalCategoryID","FeedbackType","FeedbackTypeID","TrainingRecommendation","TrainingRecommendationID",
    "HireDate","TerminationDate","LastPromotionDate","GoalAchievementPercent","ReviewApproveDate","ReviewSubmissionDate",
    "ReviewerName","ReviewerID","StressLevelScore","ImprovementPlan","ImprovementPlanID","PromotionCount","TeamSize",
    "WorkFromHomeDays","RetentionScore","EngagementScore","InternalPromotion","InternalPromotionID",
    "Participation_in_Initiatives","InitiativeParticipationID","PerformancePreference","PerformancePreferenceID",
    "MentalHealth_Support_Usage","MentalHealthSupportUsageID","CulturalFit","CulturalFitID","TeamDynamics","TeamDynamicsID",
    "Feedback","FeedbackID","AbsenceDays","EngagementLevel","EngagementLevelID","InternalTransferStatus","InternalTransferStatusID",
    "Mobility","MobilityID","WorkplaceSupport_Level","WorkplaceSupportLevelID","Policy_Violations","PolicyViolationID",
    "ComplianceTrainingStatus","ComplianceTrainingStatusID","CareerPath","CareerPathID","Total Work Experience","WorkExperienceID",
    "Recruitment_Channel","RecruitmentChannelID","Project","ProjectID","Asset","AssetID","Shift","ShiftID",
    "Leave_Type","LeaveTypeID","Leave_Status","LeaveStatusID","Attendance_Status","AttendanceStatusID",
    "Bank","BankID","Trainer Name","TrainerID","Training_Type","TrainingTypeID","Training_Location","TrainingLocationID",
    "Feedback","Feedback_ID","Exit_Reason","ExitReasonID","Days_Worked","DaysWorkedID"
]

# ---------- ROW GENERATION ----------
def generate_row(e_num):
    em = EMPLOYEES[e_num]

    bonus = round(random.uniform(0, em["BaseSalary"] * 0.5), 2)
    hire_date = dt.date.fromisoformat(em["DateOfHire"])
    tenure_month = max(0, (TODAY.year - hire_date.year) * 12 + (TODAY.month - hire_date.month))

    row = [
        em["EmployeeID"], em["FirstName"], em["LastName"]
    ]

    # Add domain values + IDs
    for domain in [
        "Gender","Department","JobTitle","EmploymentStatus","Location","EducationLevel",
        "MaritalStatus","Nationality","CertificationName","EmploymentType",
        "CertificationStatus","TrainingProgram","SkillCategory","ReviewerRole","LearningPath",
        "CertificationProvider","CertificationLevel","ReviewPeriod","PromotionEligibility",
        "ReviewType","ReviewStatus","ReviewLocation","CompetencyCategory","GoalCategory",
        "FeedbackType","TrainingRecommendation","ImprovementPlan","InternalPromotion",
        "InitiativeParticipation","PerformancePreference","MentalHealthSupportUsage","CulturalFit",
        "TeamDynamics","Feedback","EngagementLevel","InternalTransferStatus","Mobility",
        "WorkplaceSupportLevel","PolicyViolations","ComplianceTrainingStatus","CareerPath",
        "WorkExperience","RecruitmentChannel","Project","Asset","Shift","LeaveType",
        "LeaveStatus","AttendanceStatus","Bank","TrainerName","TrainingType","TrainingLocation",
        "Feedback","ExitReason","DaysWorked"
    ]:
        val = random.choice(DOMAINS[domain])
        row.append(val)
        row.append(LOOKUPS[domain][val])

    # Add numeric/other fields
    cert_date = random_date(2015, 2025)
    skill_score = round(random.uniform(0, 100), 2)
    train_hours = random.randint(0, 40)
    train_score = round(random.uniform(40, 100), 2)
    train_feedback_score = round(random.uniform(1, 5), 1)
    renewal_required = random.choice(["Yes", "No"])
    renewal_que_date = random_date(2025, 2026)
    perf_score = round(random.uniform(1, 5), 1)
    goal_ach_percent = round(random.uniform(0, 100), 2)
    stress_score = round(random.uniform(0, 10), 2)
    promotion_count = random.randint(0, 5)
    team_size = random.randint(1, 50)
    wfh_days = random.randint(0, 5)
    retention_score = round(random.uniform(0, 100), 2)
    engagement_score = round(random.uniform(0, 100), 2)
    absence_days = random.randint(0, 20)
    days_worked_num = random.randint(50, 365)

    # Dates
    term_date = random_date_between(hire_date, TODAY) if random.random() < 0.2 else ""
    last_promo_date = random_date_between(hire_date, TODAY) if random.random() < 0.5 else ""
    review_approve_date = random_date(2020, 2025)
    review_sub_date = random_date(2020, 2025)
    hire_date_out = hire_date

    # Manager
    reviewer_name = random.choice(FIRST_NAMES) + " " + random.choice(LAST_NAMES)
    reviewer_id = f"REV{random.randint(1,9999):04d}"

    # Add static + numeric values in order
    insert_values = [
        em["BaseSalary"], bonus, tenure_month, iso(cert_date),
        train_hours, train_score, skill_score, train_feedback_score,
        renewal_required, iso(renewal_que_date), perf_score,
        goal_ach_percent, iso(hire_date_out), iso(term_date), iso(last_promo_date),
        iso(review_approve_date), iso(review_sub_date),
        reviewer_name, reviewer_id, stress_score, promotion_count,
        team_size, wfh_days, retention_score, engagement_score,
        absence_days, days_worked_num
    ]
    row.extend(insert_values)

    return row

# ---------- CSV GENERATION ----------
def generate_csv():
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)
    chunks = ceil(TOTAL_ROWS / CHUNK_SIZE)
    written = 0
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for c in range(chunks):
            batch = []
            for _ in range(CHUNK_SIZE if (written+CHUNK_SIZE) <= TOTAL_ROWS else TOTAL_ROWS-written):
                e_num = random.randint(1, NUM_EMPLOYEES)
                batch.append(generate_row(e_num))
            writer.writerows(batch)
            written += len(batch)
            print(f"Chunk {c+1}/{chunks} written. Total rows so far: {written}")
    print(f"Done. File: {OUTPUT_CSV} Rows: {written}")

if __name__ == "__main__":
    generate_csv()
