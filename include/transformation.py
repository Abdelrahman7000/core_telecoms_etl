import pandas as pd

def transform_customers_data(df):

    df.columns = (
            df.columns
            .str.lower()           # make lowercase
            .str.strip()           # remove leading/trailing spaces
            .str.replace(" ", "_") # replace spaces with underscore
        )
    df['signup_date']=pd.to_datetime(df['signup_date'], format='%Y-%m-%d')
    df["email"] = df["email"].str.lower()
    email_fixes = {
        "gmail.om": "gmail.com",
        "gmai.com": "gmail.com",
        "gamil.com": "gmail.com",
        "hotmai.com": "hotmail.com",
        "hotmai.co": "hotmail.com",
        "yaho.com": "yahoo.com"
    }

    for wrong, right in email_fixes.items():
        df["email"] = df["email"].str.replace(wrong, right, regex=False)
    return df

def transform_agents_data(df):

    df.columns = (
            df.columns
            .str.lower()           # make lowercase
            .str.strip()           # remove leading/trailing spaces
            .str.replace(" ", "_") # replace spaces with underscore
        )
    return df

def transform_call_centers_logs(df):

    df.columns = (
            df.columns
            .str.lower()           # make lowercase
            .str.strip()           # remove leading/trailing spaces
            .str.replace(" ", "_") # replace spaces with underscore
        )
    rename_dic = {
        "complaint_catego_ry": "complaint_category",
        "resolutionstatus": "resolution_status",
        "calllogsgenerationdate": "call_logs_generation_date"
    }
    df = df.rename(columns=rename_dic)
    df['call_logs_generation_date']=pd.to_datetime(df['call_logs_generation_date'], format='%Y-%m-%d')
    return df

def transform_web_forms(df):

    df.columns = (
            df.columns
            .str.lower()           # make lowercase
            .str.strip()           # remove leading/trailing spaces
            .str.replace(" ", "_") # replace spaces with underscore
        )
    rename_dic = {
        "complaint_catego_ry": "complaint_category",
        "resolutionstatus": "resolution_status",
        "webformgenerationdate": "web_form_generation_date"
    }
    df = df.rename(columns=rename_dic)
    df['web_form_generation_date']=pd.to_datetime(df['web_form_generation_date'], format='%Y-%m-%d')
    return df

def transform_media_complaints(df):

    df.columns = (
            df.columns
            .str.lower()           # make lowercase
            .str.strip()           # remove leading/trailing spaces
            .str.replace(" ", "_") # replace spaces with underscore
        )
    rename_dic = {
        "complaint_catego_ry": "complaint_category",
        "resolutionstatus": "resolution_status",
        "mediacomplaintgenerationdate": "media_complaint_generation_date"
    }
    df = df.rename(columns=rename_dic)
    return df
