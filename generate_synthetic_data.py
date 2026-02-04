"""
Synthetic Data Generator for Insurance Use Case
Generates realistic insurance member data with proper distributions
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
NUM_MEMBERS = 10000
NUM_CLAIMS = 50000
NUM_CALLS = 100000
NUM_DIGITAL = 150000
NUM_SURVEYS = 25000
NUM_PHARMACY = 75000
NUM_ENROLLMENT = 15000

# Date range
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)

def generate_members():
    """Generate member demographics data"""
    print("Generating members.csv...")
    
    plan_types = ['MAPD', 'PPO', 'HMO', 'POS', 'EPO']
    segments = ['High Value', 'Medium Value', 'Low Value', 'At Risk', 'New']
    states = ['CA', 'TX', 'FL', 'NY', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI']
    
    members = []
    for i in range(NUM_MEMBERS):
        member_id = f"M{str(i+1).zfill(8)}"
        age = np.random.choice([65, 70, 75, 80, 85], p=[0.3, 0.25, 0.2, 0.15, 0.1])
        age = age + np.random.randint(-2, 3)  # Add some variance
        gender = np.random.choice(['M', 'F'], p=[0.45, 0.55])
        plan_type = np.random.choice(plan_types, p=[0.35, 0.25, 0.20, 0.12, 0.08])
        tenure_months = np.random.choice([6, 12, 24, 36, 60, 120], p=[0.15, 0.20, 0.25, 0.20, 0.15, 0.05])
        tenure_months = tenure_months + np.random.randint(-3, 4)
        segment = np.random.choice(segments, p=[0.20, 0.35, 0.25, 0.12, 0.08])
        state = np.random.choice(states, p=[0.12, 0.10, 0.11, 0.09, 0.08, 0.08, 0.08, 0.09, 0.08, 0.17])
        zip_code = str(np.random.randint(10000, 99999))
        join_date = START_DATE + timedelta(days=np.random.randint(0, 365*3))
        
        members.append({
            'member_id': member_id,
            'age': age,
            'gender': gender,
            'plan_type': plan_type,
            'tenure_months': tenure_months,
            'segment': segment,
            'state': state,
            'zip_code': zip_code,
            'join_date': join_date.strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(members)
    df.to_csv('/home/runner/work/insurance_use_case/insurance_use_case/data/members.csv', index=False)
    print(f"Generated {len(df)} members")
    return df

def generate_claims(members_df):
    """Generate claims data"""
    print("Generating claims.csv...")
    
    member_ids = members_df['member_id'].tolist()
    claim_types = ['Medical', 'Pharmacy', 'Preventive', 'Emergency', 'Hospital', 'Specialist']
    statuses = ['Approved', 'Denied', 'Pending', 'Under Review']
    denial_reasons = ['Pre-auth Required', 'Out of Network', 'Not Covered', 'Duplicate', 'Incomplete Info', None]
    
    claims = []
    for i in range(NUM_CLAIMS):
        claim_id = f"C{str(i+1).zfill(10)}"
        member_id = random.choice(member_ids)
        claim_date = START_DATE + timedelta(days=np.random.randint(0, (END_DATE - START_DATE).days))
        claim_type = np.random.choice(claim_types, p=[0.30, 0.25, 0.15, 0.12, 0.10, 0.08])
        claim_amount = np.random.lognormal(6, 1.5)  # Log-normal distribution for claims
        claim_amount = round(max(50, min(100000, claim_amount)), 2)
        status = np.random.choice(statuses, p=[0.75, 0.15, 0.07, 0.03])
        approval_time_days = np.random.choice([1, 2, 3, 5, 7, 14, 30]) if status != 'Pending' else None
        denial_reason = np.random.choice(denial_reasons) if status == 'Denied' else None
        
        claims.append({
            'claim_id': claim_id,
            'member_id': member_id,
            'claim_date': claim_date.strftime('%Y-%m-%d'),
            'claim_type': claim_type,
            'claim_amount': claim_amount,
            'status': status,
            'approval_time_days': approval_time_days,
            'denial_reason': denial_reason
        })
    
    df = pd.DataFrame(claims)
    df.to_csv('/home/runner/work/insurance_use_case/insurance_use_case/data/claims.csv', index=False)
    print(f"Generated {len(df)} claims")
    return df

def generate_digital_interactions(members_df):
    """Generate digital interaction data with realistic failure patterns"""
    print("Generating digital_interactions.csv...")
    
    member_ids = members_df['member_id'].tolist()
    interaction_types = ['login', 'bill_pay', 'claims_check', 'id_card_download', 'benefits_lookup', 
                         'find_provider', 'update_info', 'pharmacy_lookup']
    outcomes = ['success', 'failure', 'abandoned']
    failure_reasons = ['Login Failed', 'Session Timeout', 'Payment Error', 'System Error', 
                       'Incomplete Form', 'Network Error', None]
    
    interactions = []
    for i in range(NUM_DIGITAL):
        interaction_id = f"D{str(i+1).zfill(10)}"
        member_id = random.choice(member_ids)
        interaction_date = START_DATE + timedelta(days=np.random.randint(0, (END_DATE - START_DATE).days))
        # Hour distribution that sums to 1.0
        hour_probs = [0.01]*6 + [0.03]*2 + [0.06]*10 + [0.04]*4 + [0.02]*2
        hour_probs = np.array(hour_probs) / np.sum(hour_probs)
        hour = int(np.random.choice(range(24), p=hour_probs))
        interaction_timestamp = interaction_date + timedelta(hours=hour, minutes=int(np.random.randint(0, 60)))
        
        # Create realistic patterns - id_card and bill_pay have higher failure rates
        interaction_type = np.random.choice(interaction_types, 
                                           p=[0.20, 0.22, 0.18, 0.15, 0.12, 0.07, 0.04, 0.02])
        
        # Higher failure rates for specific types
        if interaction_type in ['id_card_download', 'bill_pay']:
            outcome = np.random.choice(outcomes, p=[0.55, 0.30, 0.15])  # Higher failure
        else:
            outcome = np.random.choice(outcomes, p=[0.75, 0.15, 0.10])  # Normal success
            
        failure_reason = np.random.choice(failure_reasons) if outcome == 'failure' else None
        session_duration = np.random.randint(30, 600) if outcome == 'success' else np.random.randint(10, 180)
        
        interactions.append({
            'interaction_id': interaction_id,
            'member_id': member_id,
            'interaction_date': interaction_date.strftime('%Y-%m-%d'),
            'interaction_timestamp': interaction_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'interaction_type': interaction_type,
            'outcome': outcome,
            'failure_reason': failure_reason,
            'session_duration_seconds': session_duration
        })
    
    df = pd.DataFrame(interactions)
    df.to_csv('/home/runner/work/insurance_use_case/insurance_use_case/data/digital_interactions.csv', index=False)
    print(f"Generated {len(df)} digital interactions")
    return df

def generate_call_center(members_df, digital_df):
    """Generate call center data with digital leakage patterns"""
    print("Generating call_center.csv...")
    
    member_ids = members_df['member_id'].tolist()
    call_topics = ['ID Card Request', 'Billing Question', 'Claims Status', 'Benefits Info', 
                   'Pharmacy Coverage', 'Provider Search', 'Prior Auth', 'Enrollment Change',
                   'Technical Support', 'General Inquiry']
    sentiments = ['positive', 'neutral', 'negative']
    
    # Get failed digital interactions to simulate leakage
    failed_digital = digital_df[digital_df['outcome'].isin(['failure', 'abandoned'])].copy()
    failed_digital['interaction_timestamp'] = pd.to_datetime(failed_digital['interaction_timestamp'])
    
    calls = []
    leakage_count = 0
    
    for i in range(NUM_CALLS):
        call_id = f"CALL{str(i+1).zfill(10)}"
        
        # 40% of calls are from digital leakage
        if i < NUM_CALLS * 0.40 and len(failed_digital) > 0:
            # Pick a failed digital interaction
            failed_row = failed_digital.sample(1).iloc[0]
            member_id = failed_row['member_id']
            # Call happens within 24 hours of digital failure
            call_timestamp = failed_row['interaction_timestamp'] + timedelta(hours=np.random.randint(1, 24))
            call_date = call_timestamp.date()
            prior_digital_attempt = True
            
            # Map interaction type to call topic
            topic_map = {
                'id_card_download': 'ID Card Request',
                'bill_pay': 'Billing Question',
                'claims_check': 'Claims Status',
                'benefits_lookup': 'Benefits Info',
                'pharmacy_lookup': 'Pharmacy Coverage',
                'find_provider': 'Provider Search'
            }
            call_topic = topic_map.get(failed_row['interaction_type'], 'Technical Support')
            leakage_count += 1
        else:
            member_id = random.choice(member_ids)
            call_date = START_DATE + timedelta(days=np.random.randint(0, (END_DATE - START_DATE).days))
            # Hour distribution that sums to 1.0
            hour_probs = [0.05, 0.07, 0.10, 0.12, 0.10, 0.08, 0.10, 0.12, 0.10, 0.08, 0.05, 0.03]
            hour_probs = np.array(hour_probs) / np.sum(hour_probs)
            hour = int(np.random.choice(range(8, 20), p=hour_probs))
            call_timestamp = datetime.combine(call_date, datetime.min.time()) + timedelta(hours=hour, minutes=int(np.random.randint(0, 60)))
            call_topic = np.random.choice(call_topics, p=[0.24, 0.22, 0.15, 0.12, 0.10, 0.06, 0.05, 0.03, 0.02, 0.01])
            prior_digital_attempt = False
        
        # Handle time varies by topic
        if call_topic in ['ID Card Request', 'Billing Question']:
            handle_time = np.random.normal(8, 3)
        elif call_topic in ['Pharmacy Coverage', 'Prior Auth']:
            handle_time = np.random.normal(15, 5)
        else:
            handle_time = np.random.normal(10, 4)
        handle_time = max(2, min(45, handle_time))
        
        # Sentiment - higher negative for certain topics
        if call_topic in ['Billing Question', 'Prior Auth', 'Technical Support']:
            sentiment = np.random.choice(sentiments, p=[0.20, 0.47, 0.33])
        else:
            sentiment = np.random.choice(sentiments, p=[0.35, 0.50, 0.15])
        
        # Repeat calls - 23.5% of calls
        is_repeat = np.random.random() < 0.235
        
        # Transfer and resolution
        transferred = np.random.random() < 0.15
        resolved = np.random.random() < 0.82
        
        calls.append({
            'call_id': call_id,
            'member_id': member_id,
            'call_date': call_date.strftime('%Y-%m-%d') if hasattr(call_date, 'strftime') else call_date,
            'call_timestamp': call_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'call_topic': call_topic,
            'handle_time_minutes': round(handle_time, 2),
            'sentiment': sentiment,
            'is_repeat_call': is_repeat,
            'prior_digital_attempt': prior_digital_attempt,
            'transferred': transferred,
            'resolved': resolved
        })
    
    df = pd.DataFrame(calls)
    df.to_csv('/home/runner/work/insurance_use_case/insurance_use_case/data/call_center.csv', index=False)
    print(f"Generated {len(df)} calls ({leakage_count} from digital leakage)")
    return df

def generate_surveys(members_df, calls_df):
    """Generate survey data"""
    print("Generating surveys.csv...")
    
    member_ids = members_df['member_id'].tolist()
    survey_types = ['post_call', 'post_digital', 'periodic', 'annual']
    feedback_texts = [
        'Great service, very helpful',
        'Too long wait time',
        'Could not find what I needed online',
        'Agent was knowledgeable',
        'Website is confusing',
        'Billing is unclear',
        'Very satisfied with my plan',
        'Process is too complicated',
        'Quick and easy',
        'Had to call multiple times'
    ]
    
    surveys = []
    for i in range(NUM_SURVEYS):
        survey_id = f"S{str(i+1).zfill(8)}"
        member_id = random.choice(member_ids)
        survey_date = START_DATE + timedelta(days=np.random.randint(0, (END_DATE - START_DATE).days))
        survey_type = np.random.choice(survey_types, p=[0.40, 0.30, 0.20, 0.10])
        
        # NPS score (0-10)
        nps_score = np.random.choice(range(11), p=[0.05, 0.05, 0.05, 0.05, 0.08, 0.10, 0.12, 0.15, 0.15, 0.12, 0.08])
        
        # CSAT score (1-5)
        csat_score = np.random.choice([1, 2, 3, 4, 5], p=[0.08, 0.10, 0.25, 0.35, 0.22])
        
        # Effort score (1-7, lower is better)
        effort_score = np.random.choice([1, 2, 3, 4, 5, 6, 7], p=[0.15, 0.20, 0.25, 0.20, 0.10, 0.06, 0.04])
        
        feedback_text = random.choice(feedback_texts) if np.random.random() < 0.7 else None
        
        surveys.append({
            'survey_id': survey_id,
            'member_id': member_id,
            'survey_date': survey_date.strftime('%Y-%m-%d'),
            'nps_score': nps_score,
            'csat_score': csat_score,
            'effort_score': effort_score,
            'feedback_text': feedback_text,
            'survey_type': survey_type
        })
    
    df = pd.DataFrame(surveys)
    df.to_csv('/home/runner/work/insurance_use_case/insurance_use_case/data/surveys.csv', index=False)
    print(f"Generated {len(df)} surveys")
    return df

def generate_pharmacy(members_df):
    """Generate pharmacy transaction data"""
    print("Generating pharmacy.csv...")
    
    member_ids = members_df['member_id'].tolist()
    drug_names = ['Metformin', 'Lisinopril', 'Atorvastatin', 'Levothyroxine', 'Amlodipine',
                  'Metoprolol', 'Albuterol', 'Omeprazole', 'Losartan', 'Gabapentin',
                  'Hydrocodone', 'Sertraline', 'Simvastatin', 'Montelukast', 'Furosemide']
    pharmacy_types = ['retail', 'mail', 'specialty']
    denied_reasons = ['Not on Formulary', 'Prior Auth Required', 'Quantity Limit', 'Step Therapy', None]
    
    pharmacy = []
    for i in range(NUM_PHARMACY):
        rx_id = f"RX{str(i+1).zfill(10)}"
        member_id = random.choice(member_ids)
        fill_date = START_DATE + timedelta(days=np.random.randint(0, (END_DATE - START_DATE).days))
        drug_name = random.choice(drug_names)
        
        # Copay varies by drug and type
        copay_base = np.random.choice([5, 10, 15, 25, 35, 50, 75, 100], p=[0.15, 0.20, 0.20, 0.15, 0.12, 0.10, 0.05, 0.03])
        copay_amount = round(copay_base + np.random.uniform(-2, 2), 2)
        
        days_supply = np.random.choice([30, 60, 90], p=[0.50, 0.30, 0.20])
        pharmacy_type = np.random.choice(pharmacy_types, p=[0.60, 0.30, 0.10])
        
        # 8% denial rate
        denied_flag = np.random.random() < 0.08
        denied_reason = np.random.choice(denied_reasons[:4]) if denied_flag else None
        
        pharmacy.append({
            'rx_id': rx_id,
            'member_id': member_id,
            'fill_date': fill_date.strftime('%Y-%m-%d'),
            'drug_name': drug_name,
            'copay_amount': copay_amount,
            'days_supply': days_supply,
            'pharmacy_type': pharmacy_type,
            'denied_flag': denied_flag,
            'denied_reason': denied_reason
        })
    
    df = pd.DataFrame(pharmacy)
    df.to_csv('/home/runner/work/insurance_use_case/insurance_use_case/data/pharmacy.csv', index=False)
    print(f"Generated {len(df)} pharmacy transactions")
    return df

def generate_enrollment(members_df):
    """Generate enrollment data"""
    print("Generating enrollment.csv...")
    
    member_ids = members_df['member_id'].tolist()
    plan_types = ['MAPD', 'PPO', 'HMO', 'POS', 'EPO']
    change_types = ['new', 'renewal', 'change']
    change_reasons = ['New Enrollment', 'Annual Renewal', 'Plan Change', 'Cost Reduction', 
                      'Provider Network', 'Benefit Enhancement', 'Moved', None]
    
    enrollments = []
    for i in range(NUM_ENROLLMENT):
        enrollment_id = f"E{str(i+1).zfill(8)}"
        member_id = random.choice(member_ids)
        enrollment_date = START_DATE + timedelta(days=np.random.randint(0, (END_DATE - START_DATE).days))
        plan_type = random.choice(plan_types)
        premium_amount = round(np.random.uniform(0, 350), 2)
        change_type = np.random.choice(change_types, p=[0.30, 0.50, 0.20])
        
        if change_type == 'new':
            change_reason = 'New Enrollment'
        elif change_type == 'renewal':
            change_reason = 'Annual Renewal'
        else:
            change_reason = np.random.choice(change_reasons[2:])
        
        enrollments.append({
            'enrollment_id': enrollment_id,
            'member_id': member_id,
            'enrollment_date': enrollment_date.strftime('%Y-%m-%d'),
            'plan_type': plan_type,
            'premium_amount': premium_amount,
            'change_type': change_type,
            'change_reason': change_reason
        })
    
    df = pd.DataFrame(enrollments)
    df.to_csv('/home/runner/work/insurance_use_case/insurance_use_case/data/enrollment.csv', index=False)
    print(f"Generated {len(df)} enrollments")
    return df

def main():
    """Main function to generate all synthetic data"""
    print("Starting synthetic data generation...")
    print("=" * 60)
    
    # Generate data in order (dependencies)
    members_df = generate_members()
    claims_df = generate_claims(members_df)
    digital_df = generate_digital_interactions(members_df)
    calls_df = generate_call_center(members_df, digital_df)
    surveys_df = generate_surveys(members_df, calls_df)
    pharmacy_df = generate_pharmacy(members_df)
    enrollment_df = generate_enrollment(members_df)
    
    print("=" * 60)
    print("✅ All synthetic data generated successfully!")
    print("\nData Summary:")
    print(f"  - Members: {len(members_df):,}")
    print(f"  - Claims: {len(claims_df):,}")
    print(f"  - Digital Interactions: {len(digital_df):,}")
    print(f"  - Call Center: {len(calls_df):,}")
    print(f"  - Surveys: {len(surveys_df):,}")
    print(f"  - Pharmacy: {len(pharmacy_df):,}")
    print(f"  - Enrollment: {len(enrollment_df):,}")
    print("\nAll files saved to /home/runner/work/insurance_use_case/insurance_use_case/data/")

if __name__ == "__main__":
    main()
