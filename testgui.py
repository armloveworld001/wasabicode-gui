import streamlit as st
from boto3 import client
from botocore.exceptions import ClientError
from datetime import datetime, timezone

# UI ส่วนบน
st.title("Wasabi S3 Version Cleaner")
st.markdown("ใส่ค่าการเชื่อมต่อ และตั้งค่า retention เพื่อลบ object เก่าๆ ออกจาก bucket")

# Input fields
aws_access_key_id = st.text_input("Wasabi Access Key ID", type="default")
aws_secret_access_key = st.text_input("Wasabi Secret Access Key", type="password")
bucket = st.text_input("Bucket Name")
prefix = st.text_input("Prefix (ถ้ามี)", "")
endpoint = st.text_input("Endpoint URL", "https://s3.ap-southeast-1.wasabisys.com/")
delete_after_retention_days = st.number_input("Delete after (days)", min_value=0, value=1)

# Run button
if st.button("Run Cleanup"):
    today = datetime.now(timezone.utc)
    try:
        s3_client = client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
        st.success("✅ Connected to Wasabi S3")
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")
        st.stop()

    try:
        s3_client.list_buckets()
    except ClientError:
        st.error("❌ Invalid Access or Secret key")
        st.stop()

    object_response_paginator = s3_client.get_paginator('list_object_versions')
    operation_parameters = {'Bucket': bucket}
    if prefix:
        operation_parameters['Prefix'] = prefix

    delete_list = []
    count_current, count_non_current = 0, 0

    st.write(f"🔍 Scanning bucket: **{bucket}**")
    for object_response_itr in object_response_paginator.paginate(**operation_parameters):
        if 'DeleteMarkers' in object_response_itr:
            for delete_marker in object_response_itr['DeleteMarkers']:
                if (today - delete_marker['LastModified']).days > delete_after_retention_days:
                    delete_list.append({'Key': delete_marker['Key'], 'VersionId': delete_marker['VersionId']})

        if 'Versions' in object_response_itr:
            for version in object_response_itr['Versions']:
                if version["IsLatest"]:
                    count_current += 1
                else:
                    count_non_current += 1
                    if (today - version['LastModified']).days > delete_after_retention_days:
                        delete_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})

    st.info(f"Before delete → Current: {count_current}, Non-current: {count_non_current}")

    if delete_list:
        st.warning(f"Deleting {len(delete_list)} objects...")
        for i in range(0, len(delete_list), 1000):
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={
                    'Objects': delete_list[i:i + 1000],
                    'Quiet': True
                }
            )
            st.json(response)
    else:
        st.success("No objects to delete based on retention policy.")

    # recount
    count_current, count_non_current = 0, 0
    for object_response_itr in object_response_paginator.paginate(Bucket=bucket):
        if 'Versions' in object_response_itr:
            for version in object_response_itr['Versions']:
                if version["IsLatest"]:
                    count_current += 1
                else:
                    count_non_current += 1

    st.success(f"After delete → Current: {count_current}, Non-current: {count_non_current}")
    st.balloons()
