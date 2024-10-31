import re
from datetime import datetime


def extract_file_name(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]

    return position

def extract_position(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]

    return position


def extract_class_code(file_content):

    try:
        classcode_match = re.search(r'(Class Code: )\s+(\d+)',file_content)
        classcode = classcode_match.group(2) if classcode_match else None
        return classcode
    except Exception as e:
        raise ValueError(f'Error extracting class code : {e}')

def extract_start_date(file_content):
    try:
        opendate_match = re.search(r'Open [Dd]ate:\s+(\d{2}-\d{2}-\d{2})', file_content)
        start_date = datetime.strptime(opendate_match.group(1), '%m-%d-%y').date() if opendate_match else None
        return start_date

    except Exception as e:
        raise ValueError(f'Error extracting starting date: {e}')

def extract_salary(file_content):
    try:
        salary_pattern = r"\$(\d{1,3}(?:,\d{3})*)\s+to\s+\$(\d{1,3}(?:,\d{3})*)"
        salary_match = re.search(salary_pattern,file_content)

        if salary_match:
            salary_start = float(salary_match.group(1).replace(',',''))
            salary_end = float(salary_match.group(2).replace(',',''))

        else:
            salary_start, salary_end = None, None

        return salary_start,salary_end

    except Exception as e:
        raise ValueError(f'Error extracting salary details : {e}')


def extract_requirements(file_content):

    try:
        requirements_pattern = r"Requirements:\s*((?:.|\n)*?)(?=DUTIES)"
        requirements_match = re.search(requirements_pattern, file_content,re.DOTALL)
        return requirements_match.group(1).strip() if requirements_match else None

    except Exception as e:
        raise ValueError(f'Error extracting requirements: {e}')

def extract_notes(file_content):

    try:
        notes_pattern = r"NOTE:\s*((?:.|\n)*?)(?=Application Location)"
        notes_match = re.search(notes_pattern, file_content)
        return notes_match.group(1).strip() if notes_match else None

    except Exception as e:
        raise ValueError(f'Error extracting notes: {e}')

def extract_duties(file_content):

    try:
        duties_pattern = r"DUTIES\s*((?:.|\n)*?)(?=Selection Criteria)"
        duties_match = re.search(duties_pattern, file_content)
        return duties_match.group(1).strip().split('\n') if duties_match else None

    except Exception as e:
        raise ValueError(f'Error extracting duties: {e}')

def extract_selection(file_content):

    try:
        selection_pattern = r"Selection Criteria:\s*((?:.|\n)*?)(?=NOTE)"
        selection_match = re.search(selection_pattern, file_content)
        return selection_match.group(1).strip() if selection_match else None

    except Exception as e:
        raise ValueError(f'Error extracting selection criteria: {e}')

def extract_experience_length(file_content):

    try:
        experience_pattern = r"(\d+)\s+years of experience"
        experience_match = re.search(experience_pattern, file_content)
        return int(experience_match.group(1)) if experience_match else None

    except Exception as e:
        raise ValueError(f'Error extracting experience length: {e}')

def extract_education(file_content):

    try:
        education_pattern = r"Bachelor’s degree"
        education_match = re.search(education_pattern, file_content)
        return "Bachelor’s degree" if education_match else None

    except Exception as e:
        raise ValueError(f'Error extracting education length: {e}')

def extract_application_location(file_content):

    try:
        location_pattern = r"Application Location\s*((?:.|\n)*?)(?=Job Type)"
        location_match = re.search(location_pattern, file_content)
        return location_match.group(1).strip() if location_match else None

    except Exception as e:
        raise ValueError(f'Error extracting application location: {e}')

def extract_job_type(file_content):

    try:
        job_type_pattern = r"Job Type\s*((?:.|\n)*?)(?=APPLICATION DEADLINE)"
        job_type_match = re.search(job_type_pattern, file_content)
        return job_type_match.group(1).strip() if job_type_match else None

    except Exception as e:
        raise ValueError(f'Error extracting job type: {e}')
