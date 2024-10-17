#!/usr/bin/env python
# coding: utf-8

# ## Schoology_py
# 
# 
# 

# In[1]:


get_ipython().run_line_magic('run', 'OEA_py')


# In[2]:


import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import col, avg, desc, first,asc,sum, when, count, concat_ws, format_string, collect_list,last,round,monotonically_increasing_id,date_format,expr,explode,substring
from pyspark.sql.types import StringType
from scipy.stats import rankdata
import time


# In[ ]:


class Schoology:
    def __init__(self, workspace='dev', version='0.1'):
        self.baseurl = "https://api.schoology.com/v1/"
        self.keyvault_consumer_key = f'schoologyConsumerKey{workspace}'
        self.keyvault_oauth_signature = f'schoologyOauthSignature{workspace}'
        self.version = version

    def set_workspace(self, workspace_name):
        oea.set_workspace(workspace_name)

    def set_version(self, version):
        self.version = version

    def auth_header(self):
        oauth_consumer_key = oea._get_secret(self.keyvault_consumer_key)
        oauth_token = ""
        # Generate dynamic values
        oauth_nonce = str(random.getrandbits(64))
        oauth_timestamp = int(time.time())

        # OAuth parameters
        oauth_signature_method = "PLAINTEXT"
        oauth_version = "1.0"


        # Construct the key for HMAC-SHA1

        # Calculate the signature using HMAC-SHA1
        oauth_signature = oea._get_secret(self.keyvault_oauth_signature)

        # Construct the Authorization header
        auth_header = (
        'OAuth realm="Schoology API", '
        f'oauth_consumer_key="{oauth_consumer_key}", '
        f'oauth_nonce="{oauth_nonce}", '
        f'oauth_timestamp="{oauth_timestamp}", '
        f'oauth_signature_method="{oauth_signature_method}", '
        f'oauth_version="{oauth_version}", '
        f'oauth_signature="{oauth_signature}"'
        )
        return auth_header

    def fetch_data(self, dataurl, resultName = 'user'):
        data_list = []
        while dataurl:
            header = self.auth_header()
            response = requests.get(dataurl, headers={"Authorization": header})
            if response.status_code == 200:
                data = response.json()
                data_list.extend(data[resultName])  # Adjust this based on the structure of your API response
                dataurl = data["links"].get("next", None) if "links" in data else None
            else:
                print(f"Error: {response.status_code}")
                break
        return data_list
    
    def load_users(self):
        currentDate = datetime.datetime.now()
        currentDateTime = currentDate.strftime("%Y-%m-%d")
        data = self.fetch_data(f'{self.baseurl}users')
        inactive_users = self.fetch_data(f'{self.baseurl}users/inactive')
        for user in inactive_users:
            user['name_display'] = user.pop('name')         # name_dispaly column to match with data 
        all_users = data + inactive_users
        # oea.land(json.dumps(all_users), f'schoology_raw/v{self.version}/users_inactive', 'inactive_users_data.json', oea.DELTA_BATCH_DATA, currentDateTime)
        oea.land(json.dumps(all_users), f'schoology_raw/v{self.version}/users', 'users.json', oea.DELTA_BATCH_DATA, currentDateTime)
       

    def load_roles(self):
        currentDate = datetime.datetime.now()
        currentDateTime = currentDate.strftime("%Y-%m-%d")
        data = self.fetch_data(f'{self.baseurl}roles', 'role')
        oea.land(json.dumps(data), f'schoology_raw/v{self.version}/roles', 'roles.json', oea.DELTA_BATCH_DATA, currentDateTime)
        

    def csv_files_list(self, path, details_list):
        try:
            items = mssparkutils.fs.ls(oea.to_prelanding_url(path))
            for item in items:
                if item.isFile and item.name.endswith('.csv'):
                    details_list.append([path, item.name])
                elif item.isDir:
                    self.csv_files_list(path + "/" + item.name, details_list)
        except Exception as e:
            logger.warning("[OEA] Could not get list of folders in specified path: " + path + "nThis may be because the path does not exist.")
        


    # Call the function to gather details list
    def get_files_from_bulk(self, path):
        details_list = []
        self.csv_files_list(path,details_list)
        # Process the CSV files one by one
        for folder_path, filename in details_list:
            self.preland_schoology_csv(folder_path,filename)


    def preprocess_schoology_dataset(self, tables_source):
        """ Stage1/Transactional/Schoology_raw/{version}/{item}/oea.SNAPSHOT_BATCH_DATA/rundate=...   ==> Stage1/Transactional/Schoology/{version}/{item}/oea.SNAPSHOT_BATCH_DATA/rundate=...
        """
        # items = oea.get_folders(tables_source)
        items = ["users"]
        for item in items:
            if item == '_preprocessed_tables':
                logger.info('Ignoring existing _preprocessed_tables folder.')
            else:
                table_path = tables_source +'/'+ item
                # find the batch data type of the table
                batch_type_folder = oea.get_folders(table_path)
                batch_type = batch_type_folder[0]
                # grab only the latest folder in stage1, used to write the JSON -> CSV to the same rundate folder timestamp
                # idea is to mimic the same directory structure of tables landed in stage1
                latest_dt = oea.get_latest_runtime(f'{table_path}/{batch_type}', "rundate=%Y-%m-%d")
                latest_dt = latest_dt.strftime("%Y-%m-%d")
                if item == 'users' or item == 'roles' or item == 'standards':
                    df = spark.read.json(oea.to_url(f'{table_path}/{batch_type}/rundate={latest_dt}/*.json'))
                else:
                    # df = pd.read_csv(oea.to_url(f'{table_path}/{batch_type}/rundate={latest_dt}/*.csv'))
                    # df = spark.read.csv(oea.to_url(f'{table_path}/{batch_type}/rundate={latest_dt}/*.csv'), header=True)
                    df = spark.read.option("quote", "\"").option("escape", "\"").csv(oea.to_url(f'{table_path}/{batch_type}/rundate={latest_dt}/*.csv'), header=True)


                if item == 'users':
                    df = df.withColumn('parents', df['parents'].cast(StringType()))
                elif item == 'roles':
                    df = df.withColumn('links', df['links'].cast(StringType())) 
                elif item == 'standards':
                    df = df.withColumn('description', regexp_replace(df['description'], r'\\r\\n', ' '))
                
                
                if item in ['question_data', 'student_submissions', 'submission_summary']:
                    # split_cols = df['File_Path'].str.split('/')
                    # df['Session'] = split_cols.str[0]
                    # df['Assessment_type'] = split_cols.str[1].str.split('-').str[1].str.strip()
                    # df['Subject'] = split_cols.str[2].str.split('-').str[1].str.strip()
                    # df['Grade'] = split_cols.str[3].str.split('-').str[1].str.strip()
                    # df['Section'] = split_cols.str[4].str.strip()
                    # df['File_Name'] = split_cols.str[5]
                    # df = df.drop(columns=['File_Path']) 
                    split_cols = split(df['File_Path'], '/')
                    df=df.withColumn("Session", split_cols.getItem(0))
                    df=df.withColumn("Assessment_type", trim(split(split_cols.getItem(1),'-').getItem(1)))
                    df=df.withColumn("Subject", trim(split(split_cols.getItem(2),'- ').getItem(1)))
                    df=df.withColumn("Grade", trim(split(split_cols.getItem(3),'-').getItem(1)))
                    df=df.withColumn("Section", trim(split_cols.getItem(4)))
                    df=df.withColumn("File_Name", split_cols.getItem(5))
                    df=df.drop("File_Path")
                    

                if item == 'question_data':
                    question_df = df

                elif item == 'student_submissions':
                    teacher_dict = {
                        "Evan Markowitz, Jannette Rivera": "Jannette Rivera",
                        "Maria Baclohan, Evan Markowitz": "Maria Baclohan",
                        "Evan Markowitz, Mason Reeder": "Mason Reeder",
                        "Melaina Fijalkowski, Kathleen Tsakonas": "Kathleen Tsakonas",
                        "Evan Markowitz, Elizabeth Sedlak": "Elizabeth Sedlak",
                        "Daniel Smith, Nicole Swidarski": "Nicole Swidarski",
                        "Susanna Birdwell, Evan Markowitz": "Susanna Birdwell",
                        "Evan Markowitz, Niki Paul": "Niki Paul",
                        "Evan Markowitz, Tiffany Schaefer": "Tiffany Schaefer",
                        "Evan Markowitz, Kathleen Tsakonas": "Kathleen Tsakonas",
                        "Susanna Birdwell, Melaina Fijalkowski": "Susanna Birdwell",
                        "Daniel Smith, Heather Watkins": "Heather Watkins",
                        "Evan Markowitz, Mary Sidhom": "Mary Sidhom",
                        "Carissa Farrell, Madison Wahn": "Carissa Farrell",
                        "Mary Sidhom, Madison Wahn": "Madison Wahn",
                        "Evan Markowitz, Pattie Rossi": "Pattie Rossi",
                        "Gabriela Agostino, Sitara Qalander": "Gabriela Agostino",
                        "Sharon Long, Pattie Rossi": "Pattie Rossi",
                        "Ana Leiva, Mary Vaughn": "Mary Vaughn",
                        "Evan Markowitz, Sitara Qalander, Jannette Rivera": "Jannette Rivera",
                        "Maria Baclohan, Sharon Long": "Maria Baclohan",
                        "Gabriela Agostino, Evan Markowitz": "Gabriela Agostino",
                        "Ashley Lekhram, Evan Markowitz": "Ashley Lekhram",
                        "Ana Leiva, Evan Markowitz, Mary Vaughn": "Ana Leiva",
                        "Evan Markowitz, Nicole Swidarski": "Nicole Swidarski",
                        "Carissa Farrell, Evan Markowitz": "Carissa Farrell",
                        "Elizabeth Bennet, Melaina Fijalkowski": "Elizabeth Bennet"
                    }
                    from pyspark.sql.functions import col, when,udf

                    def replace_values(value):
                        for key, val in teacher_dict.items():
                            if value == key:
                                return val
                        return value

                    # Define the UDF using the replace_values function
                    replace_values_udf = udf(replace_values, StringType())

                    # Apply the UDF to the DataFrame
                    df = df.withColumn("Section_Instructors_values", 
                                    when(col("Section_Instructors").contains(","),
                                            replace_values_udf(col("Section_Instructors")))
                                    .otherwise(col("Section_Instructors")))
                    df = df.drop("Section_Instructors").withColumnRenamed("Section_Instructors_values", "Section_Instructors")
                    student_submissions_df = df
            
                elif item == 'submission_summary':
                    df = df.withColumnRenamed('Schoology_ID', 'User_ID')
                    df = df.withColumn('question_No', split(df['Question'], '_').getItem(1))
                    df = df.drop("Question")
                    df = df.withColumn('File_Name_Item_removed', regexp_replace(df['File_Name'], 'Submission-Summary-', ''))
                    question_df = question_df.withColumn('File_Name_QItem_removed', regexp_replace(question_df['File_Name'], 'Question-Data-', ''))

                    merged_df = df.join(question_df.select('Item_ID', 'File_Name_QItem_removed', 'Question_No', 'Question_ID').dropDuplicates(['File_Name_QItem_removed', 'Question_No']),
                            (df['File_Name_Item_removed'] == question_df['File_Name_QItem_removed']) & 
                            (df['question_No'] == question_df['Question_No']),
                            'left') 

                    df = merged_df.withColumn('Question_ID', col('Question_ID')).withColumn('Item_ID', col('Item_ID'))
                    merged_df2 = df.join(student_submissions_df.select('User_UID', 'User_School_ID').dropDuplicates(['User_UID']),
                        df['User_ID'] == student_submissions_df['User_UID'],
                        'left')
                    df = merged_df2.withColumn('School_ID', col('User_School_ID')).drop('User_UID', 'User_School_ID')
                    df = df.drop('File_Name_QItem_removed','Question_No')
                
                entity_path = f'schoology/v{self.version}/{item}'
                oea.land(df, entity_path = entity_path , rundate = f'{latest_dt}' )
                # remove the _SUCCESS file
                new_table_path = f'stage1/Transactional/schoology/v{self.version}/{item}/{batch_type}/rundate={latest_dt}'
                oea.rm_if_exists(new_table_path + '/_SUCCESS', False)
            logger.info('Finished pre-processing Schoology tables')
        

    def preland_schoology_csv(self, folderPath,fileName):
        """ pre-landing/Schoology/{tenant}/Year....   ==> Stage1/Transactional/Schoology_raw/{version}/{item}/oea.SNAPSHOT_BATCH_DATA/rundate=...
        """
        print("folder_path is: ",folderPath)
        print("file_name is: ",fileName)
        csv_file_path=oea.to_prelanding_url(folderPath+'/'+fileName)
        # df=spark.read.format("csv").option("header","true").load(csv_file_path)
        df = spark.read.format("csv").option("header", "true") .option("quote", "\"").option("escape", "\"").option("quoteAll", "true").load(csv_file_path)

        df1 = oea.fix_column_names(df)
        
        if 'Question' in df1.columns:
            # Truncate 'Question' column to 8000 characters
            df1 = df1.withColumn("Question", expr("SUBSTRING(Question, 1, 8000)"))


        pd_df = df1.toPandas()
        item = ''

        if "Question-Data" in fileName:
            item = 'question_data'
            list_all_cols = list(pd_df.columns)

            list_val_vars_Stan=[]
            [list_val_vars_Stan.append(w) if w.startswith("Standards") else "" for w in list_all_cols]

            list_id_var1 = []
            list_id_var1 = list(set(list_all_cols) - set(list_val_vars_Stan))
            
            df_melt1 = pd_df.melt(id_vars=list_id_var1, 
                        value_vars=list_val_vars_Stan,
                        var_name='Standards', value_name='Standards_Val')

            # display(df_melt1)            

            # ==========Repeating for Answer_Breakdown =====================================================

            list_all_cols_melt1 = list(df_melt1.columns)

            list_val_vars_AnsB=[]
            [list_val_vars_AnsB.append(w) if w.startswith("Answer_Breakdown") else "" for w in list_all_cols_melt1]

            list_id_var2 = list(set(list_all_cols_melt1) - set(list_val_vars_AnsB))

            # print(list_val_vars_AnsB)
            # print(list_id_var2)

            df_final = df_melt1.melt(id_vars=list_id_var2, 
                        value_vars=list_val_vars_AnsB,
                        var_name='Answer_Breakdown', value_name='Answer_Breakdown_Val')

            df_final['Unique_Key'] = (
                                        df_final['Item_ID'].astype(str) +
                                        df_final['Question_ID'].astype(str) +
                                        df_final['Position_Number'].astype(str) +
                                        df_final['Answer_Option'].astype(str) +
                                        df_final['Answer_Breakdown'].astype(str) +
                                        df_final['Standards'].astype(str)
                                        )

            df_final = df_final.sort_values(by="Question_ID").reset_index(drop=True)
            df_final['Question_No'] = rankdata(df_final['Question_ID'], method='dense').astype(int)

            df_final['File_Path'] = '/'.join(folderPath.split('/')[3:]) + '/' + fileName  
            
        elif "Submission-Summary" in fileName:
            item = 'submission_summary'
            list_all_cols = list(pd_df.columns)

            list_val_vars_Ques=[]
            [list_val_vars_Ques.append(w) if w.startswith("Question") else "" for w in list_all_cols]
            
            list_id_var1 = []
            list_id_var1 = list(set(list_all_cols) - set(list_val_vars_Ques))
            
            df_final = pd_df.melt(id_vars=list_id_var1, 
                        value_vars=list_val_vars_Ques,
                        var_name='Question', value_name='Question_Val')
            df_final['Unique_Key'] = (df_final['Schoology_ID'].astype(str) + df_final['Question'].astype(str))
            df_final['File_Path'] = '/'.join(folderPath.split('/')[3:]) + '/' + fileName  


        elif "Student-Submissions" in fileName:
            item = 'student_submissions'
            df_final = pd_df.copy()
            df_final['Unique_Key'] = (
                                    df_final['User_UID'].astype(str) +
                                    df_final['Item_ID'].astype(str) +
                                    df_final['Question_ID'].astype(str) +
                                    df_final['Position_Number'].astype(str) +
                                    df_final['Answer_Submission'].astype(str) +
                                    df_final['Correct_Answer'].astype(str)
                                )
            df_final['File_Path'] = '/'.join(folderPath.split('/')[3:]) + '/' + fileName  

        elif "Attendance" in fileName:
            item = 'attendance'
            df_final = pd_df.copy()

        
        elif "SIS" in fileName:
            item = 'student_information'
            df_final = pd_df.copy()
        
              
        destinationFolderPPath = f'stage1/Transactional/schoology_raw/v{self.version}/{item}'
        
        # Use the current date and time
        latest_dt = datetime.datetime.now().strftime("%Y-%m-%d")        
        new_table_path = f'stage1/Transactional/schoology_raw/v{self.version}/{item}/{oea.DELTA_BATCH_DATA}/rundate={latest_dt}'

        #making directories using dbutils for pandas csv file writting
        mssparkutils.fs.mkdirs(oea.to_url(new_table_path))
        df_final.to_csv(oea.to_url(f'{new_table_path}/{fileName}'))

        # spark_df = spark.createDataFrame(df_final)
        # spark_df.coalesce(1).write.save(oea.to_url(f'{new_table_path}'), format='csv', mode='overwrite', header='true', mergeSchema='true')

    def ingest_schoology_dataset(self,tables_source):
        """ Stage1/Transactional/Schoology/{version}/{item}/oea.SNAPSHOT_BATCH_DATA/rundate=...  ==> Stage2/Ingested/Schoology/{version}/{item}
        """
        # items = oea.get_folders(f'stage1/Transactional/{tables_source}')
        items=["users"]
        # items = ["question_data","student_submissions","submission_summary"]
        for item in items:
            table_path = f'schoology/v{self.version}/{item}'
            try:
                # 3 paths: check_path is for checking whether the table should be ingested, read_path is for reading the stage1 CSV location, write path for stage2 ingested location
                if item == 'metadata.csv':
                    logger.info('ignore metadata csv - not a table to be ingested')
                elif item == 'users':
                    oea.ingest(table_path, 'uid')
                elif item == 'roles':
                    oea.ingest(table_path, 'id')
                elif item == 'standards':
                    oea.ingest(table_path,'Identifier')
                elif item == "student_information" or item =="attendance":
                    oea.ingest(table_path,'Student_ID')
                else:
                    oea.ingest(table_path, 'Unique_Key')
            except AnalysisException as e:
                # This means the table may have not been properly refined due to errors with the primary key not aligning with columns expected in the lookup table.
                pass
        logger.info('Finished ingesting the most recent Schoology data')

    def refine_schoology_dataset(tables_source):
            items = oea.get_folders(tables_source)
            for item in items: 
                table_path = tables_source +'/'+ item
                if item == 'metadata.csv':
                    logger.info('ignore metadata processing, since this is not a table to be ingested')
                else:
                    try:
                        pass
                        """ TODO: No refinement currently, will do later
                        """
                    except AnalysisException as e:
                        # This means the table may have not been properly refined due to errors with the primary key not aligning with columns expected in the lookup table.
                        pass
                    
                    logger.info('Refined table: ' + item + ' from: ' + table_path)
            logger.info('Finished refining Canvas tables')

    def _publish_to_stage2(self,df, destination, pk):
        oea.upsert(df, destination, pk)

    def publish(self,df, stage2_destination, stage3_destination, primary_key='id'):
        self._publish_to_stage2(df, stage2_destination, primary_key)
        streaming_df = spark.read.format('delta').load(oea.to_url(stage2_destination))
        # for more info on append vs complete vs update modes for structured streaming: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts
        streaming_df.write.format('delta').mode('append').option("mergeSchema", "true").save(oea.to_url(stage3_destination))
        number_of_new_inbound_rows = streaming_df.count()
        logger.info(f'Number of new inbound rows processed: {number_of_new_inbound_rows}')
        return number_of_new_inbound_rows
    
    def build_dimension_tables(self,tables_source):
        """ Stage2/Ingested/Schoology/{version}/{item}  ==> Stage2/Enriched/Schoology/{version}/{item}
        """
        df_User = oea.load(f'stage2/Ingested/schoology/v{self.version}/users')
        df_Question = oea.load(f'stage2/Ingested/schoology/v{self.version}/question_data')
        df_Question = df_Question.filter((F.col('Standards_Val').isNull()) & (F.col('Standards') != 'Standards17'))
        df_Student_Submissions = oea.load(f'stage2/Ingested/schoology/v{self.version}/student_submissions')
        df_Submission_Summary = oea.load(f'stage2/Ingested/schoology/v{self.version}/submission_summary')
        df_Student_Info = oea.load(f'stage2/Ingested/schoology/v{self.version}/student_information')

        df_Student_Submissions = df_Student_Submissions.withColumnRenamed('User_School_ID','School_ID').withColumnRenamed('User_School_Name','School_Name')
        df_User = df_User.withColumnRenamed('school_id','School_ID')
        
        # dim_school
        df_dim_school = df_Student_Submissions[['School_ID','School_Name']].drop_duplicates()
        self.publish(df_dim_school, f'stage2/Enriched/schoology/v{self.version}/dim_school',f'stage3/Published/schoology/v{self.version}/dim_school', primary_key='School_ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_school')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_school')
        
        # dim_section
        df_dim_section = df_Student_Submissions[['Section_Code','Section_NID','Section_Name','Section_Instructors','School_ID']].drop_duplicates()
        self.publish(df_dim_section,f'stage2/Enriched/schoology/v{self.version}/dim_section',f'stage3/Published/schoology/v{self.version}/dim_section', primary_key='Section_NID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_section')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_section')

        # Dim_Role
        # df_Dim_Role = 
        # Role_ID	    numeric
        # Role_Name	nvarchar(max)

        # dim_student
        df_dim_student = df_User.filter(df_User['role_id'] == 286170)
        df_dim_student= df_dim_student[['uid','id','School_ID','school_uid','name_title','name_first','name_first_preferred',
        'use_preferred_first_name','name_middle','name_middle_show','name_last','name_display','primary_email','picture_url',
        'gender','position','grad_year','username','password','role_id','tz_offset','tz_name','language']].drop_duplicates()

        df_dim_stu_info = df_Student_Info.select("Student_ID", "Grade", "Home_Room_Teacher", "English_Language_Learner_ELL_", "Primary_Exceptionality", "Race")
        # Joining the dataframes
        joined_df = df_dim_student.join(df_dim_stu_info,df_dim_stu_info["Student_ID"] == df_dim_student["school_uid"],"left")
        joined_df = joined_df.drop("school_uid")
        self.publish(joined_df, f'stage2/Enriched/schoology/v{self.version}/dim_student',f'stage3/Published/schoology/v{self.version}/dim_student', primary_key='uid')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_student')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_student')


        # dim_teacher
        df_dim_teacher = df_User.filter(df_User['role_id'] == 286168)
        df_dim_teacher= df_dim_teacher[['uid','id','School_ID','name_title','name_first','name_first_preferred',
        'use_preferred_first_name','name_middle','name_middle_show','name_last','name_display','primary_email','picture_url',
        'gender','position','grad_year','username','password','role_id','tz_offset','tz_name','language']].drop_duplicates()
        self.publish(df_dim_teacher, f'stage2/Enriched/schoology/v{self.version}/dim_teacher',f'stage3/Published/schoology/v{self.version}/dim_teacher', primary_key='uid')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_teacher')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_teacher')


        # dim_parent
        df_dim_parent = df_User.filter(df_User['role_id'] == 286172)
        df_dim_parent= df_dim_parent[['uid','id','School_ID','name_title','name_first','name_first_preferred',
        'use_preferred_first_name','child_uids','name_middle','name_middle_show','name_last','name_display','primary_email','picture_url',
        'gender','position','grad_year','username','password','role_id','tz_offset','tz_name','language']].drop_duplicates()
        self.publish(df_dim_parent, f'stage2/Enriched/schoology/v{self.version}/dim_parent',f'stage3/Published/schoology/v{self.version}/dim_parent', primary_key='uid')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_parent')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_parent')


        # dim_item  
        df_dim_item = df_Student_Submissions[['Item_ID','Item_Type','Item_Name','School_ID']].drop_duplicates()
        self.publish(df_dim_item, f'stage2/Enriched/schoology/v{self.version}/dim_item',f'stage3/Published/schoology/v{self.version}/dim_item', primary_key='Item_ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_item')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_item')

        # dim_course
        df_dim_course = df_Student_Submissions[['Course_NID','Course_Name','Course_code','School_ID']].drop_duplicates()
        self.publish(df_dim_course, f'stage2/Enriched/schoology/v{self.version}/dim_course',f'stage3/Published/schoology/v{self.version}/dim_course', primary_key='Course_NID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_course')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_course')

        # dim_standard
        df_dim_standard = oea.load(f'stage2/Ingested/schoology/v{self.version}/standards')
        self.publish(df_dim_standard, f'stage2/Enriched/schoology/v{self.version}/dim_standard',f'stage3/Published/schoology/v{self.version}/dim_standard', primary_key='Identifier')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_standard')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_standard')


      # Getting question_data (df_Question_schoolID) having colun School_ID
        df_Student_Submissions_schoolID = df_Student_Submissions.select("Question_ID","School_ID")
        df_Question_schoolID = df_Question.join(df_Student_Submissions_schoolID,df_Question["Question_ID"] == df_Student_Submissions_schoolID["Question_ID"],"inner")
        df_Question_schoolID = df_Question_schoolID.select(
            [df_Question[col] for col in df_Question.columns] + [df_Student_Submissions_schoolID["School_ID"]])
       
       # dim_question_data     
        df_Question_schoolID = df_Question_schoolID.drop("Standards")
        df_Question_schoolID = df_Question_schoolID.withColumnRenamed('Standards_Val','Standards')

        df_dim_question_data = df_Question_schoolID[['Question',
        'Position_Number',
        'Item_ID',
        'Item_Name',
        'School_ID',
        'Standards',
        'Question_ID',
        'Question_No',        
        'Least_Points_Earned',
        'Correct_Answer',
        'Question_Type',
        'Average_Points_Earned',
        'Associated_Question_ID',
        'Total_Points',
        'Most_Points_Earned',
        'Correctly_Answered',
        'Sub-Question',
        'Session',
        'Assessment_type',
        'Subject',
        'Grade',
        # 'Section_NID',
        'Section'
        ]].drop_duplicates()
        df_dim_question_data = df_dim_question_data.withColumn('Qkey',concat(df_dim_question_data['Session'],df_dim_question_data['Assessment_type'],df_dim_question_data['Subject'],df_dim_question_data['Grade'],df_dim_question_data['Question_ID']))

        schoology.publish(df_dim_question_data,f'stage2/Enriched/schoology/v{self.version}/dim_question_data',f'stage3/Published/schoology/v{self.version}/dim_question_data', primary_key='Qkey')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_question_data')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_question_data')

        # dim_unit_lesson
        dim_unit_lesson=df_Student_Submissions[['Item_ID','Item_Name','School_ID',]].drop_duplicates()
        self.publish(dim_unit_lesson, f'stage2/Enriched/schoology/v{self.version}/dim_unit_lesson',f'stage3/Published/schoology/v{self.version}/dim_unit_lesson', primary_key='Item_ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_unit_lesson')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_unit_lesson')

        # first fetching student submissions table as to get rows for 3 dim tables with unique ids (Grade_ID,Assessment_ID,Subject_ID)
        df_Student_Submissions = oea.load(f'stage2/Ingested/schoology/v{self.version}/student_submissions')
        df_Student_Submissions = df_Student_Submissions.withColumnRenamed('User_School_ID','School_ID').withColumnRenamed('User_School_Name','School_Name')
        df_Student_Submissions = df_Student_Submissions[['User_UID','User_Role_ID','School_ID','Course_NID','Section_NID',
        'Section_Code','Item_ID','Item_Name','First_Access','Latest_Attempt','Total_Time','Submission_Grade','Submission','Question_ID',
        'Session','Assessment_type','Subject','Grade','Section','File_Name',
        'Position_Number','Sub-Question','Answer_Submission','Correct_Answer','Points_Received','Points_Possible']]
        #adding one new column to be a primary key
        df_Student_Submissions = df_Student_Submissions.withColumn("User_id_ques_id",concat(col("User_UID"), lit('-'), col("Question_ID")))
        df_Student_Submissions = df_Student_Submissions.withColumn('Qkey', 
            concat(col('Session'), col('Assessment_type'), col('Subject'), col('Grade'), col('Question_ID'))
        )     
        # Selecting required columns 
        fact_Student_Submissions = df_Student_Submissions.select(
            'User_UID','User_Role_ID','School_ID','Course_NID','Section_NID',
        'Section_Code','Item_ID','Item_Name','First_Access','Latest_Attempt','Total_Time','Submission_Grade','Submission','Question_ID',
        'Session','Assessment_type','Subject','Grade','Section','File_Name',
        'Position_Number','Sub-Question','Answer_Submission','Correct_Answer','Points_Received','Points_Possible','User_id_ques_id'
        )


        # dim_session
        # dim_session to create sessionID
        fact_Student_Submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission') 
        dim_session = fact_Student_Submissions[['School_ID','Session']].drop_duplicates()

        def generate_uuid(column1, column2):
            combined_column = concat(column1, column2)
            uuid = sha2(combined_column, 256)  # Apply a hashing function to generate UUIDs
            return uuid

        dim_session = dim_session.withColumn("session_ID", generate_uuid(dim_session['School_ID'], dim_session['Session']))
        dim_session = dim_session.dropna(subset=["Session_ID", "School_ID", "Session"])

        self.publish(dim_session, f'stage2/Enriched/schoology/v{self.version}/dim_session',f'stage3/Published/schoology/v{self.version}/dim_session', primary_key='Session_ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_session')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_session')


        # genearting unique id for (dim_grade,dim_assessment_type, dim_subject) after student submissions fact table
        def generate_uuid(column1, column2):
            from pyspark.sql.functions import sha2, concat_ws
            combined_column = concat(column1, column2)
            uuid = sha2(combined_column, 256)  # Apply a hashing function to generate UUIDs
            return uuid
            
        fact_Student_Submissions = fact_Student_Submissions.withColumn("Grade_ID", generate_uuid(df_Student_Submissions['School_ID'], df_Student_Submissions['Grade']))
        fact_Student_Submissions = fact_Student_Submissions.withColumn("Assessment_ID", generate_uuid(df_Student_Submissions['School_ID'], df_Student_Submissions['Assessment_type']))
        fact_Student_Submissions = fact_Student_Submissions.withColumn("Subject_ID", generate_uuid(df_Student_Submissions['School_ID'], df_Student_Submissions['Subject']))

        # dim_grade
        dim_grade = fact_Student_Submissions[['Grade_ID','School_ID','Grade']].drop_duplicates()
        self.publish(dim_grade,f'stage2/Enriched/schoology/v{self.version}/dim_grade',f'stage3/Published/schoology/v{self.version}/dim_grade', primary_key='Grade_ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_grade')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_grade')

        # dim_assessment_type
        dim_assessment_type = fact_Student_Submissions[['Assessment_ID','School_ID','Assessment_type']].drop_duplicates()
        self.publish(dim_assessment_type,f'stage2/Enriched/schoology/v{self.version}/dim_assessment_type',f'stage3/Published/schoology/v{self.version}/dim_assessment_type', primary_key='Assessment_ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_assessment_type')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_assessment_type')

        # dim_subject
        dim_subject = fact_Student_Submissions[['Subject_ID','School_ID','Subject']].drop_duplicates()
        self.publish(dim_subject,f'stage2/Enriched/schoology/v{self.version}/dim_subject',f'stage3/Published/schoology/v{self.version}/dim_subject', primary_key='Subject_ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/dim_subject')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/dim_subject') 


    def build_fact_tables(self,tables_source):
        """ Stage2/Ingested/Schoology/{version}/{item}  ==> Stage2/Enriched/Schoology/{version}/{item}
        """
                
        df_Student_Submissions = oea.load(f'stage2/Ingested/schoology/v{self.version}/student_submissions')
        df_Student_Submissions = df_Student_Submissions.withColumnRenamed('User_School_ID','School_ID').withColumnRenamed('User_School_Name','School_Name')

        df_Question = oea.load(f'stage2/Ingested/schoology/v{self.version}/question_data')
        df_Question = df_Question.filter((F.col('Standards_Val').isNull()) & (F.col('Standards') != 'Standards17'))

        # fact_student_submission          
        df_Student_Submissions = df_Student_Submissions[['User_UID','User_Role_ID','School_ID','Course_NID','Section_NID',
        'Section_Code','Item_ID','Item_Name','First_Access','Latest_Attempt','Total_Time','Submission_Grade','Submission','Question_ID',
        'Session','Assessment_type','Subject','Grade','Section','File_Name',
        'Position_Number','Sub-Question','Answer_Submission','Correct_Answer','Points_Received','Points_Possible']]
        #adding one new column to be a primary key
        df_Student_Submissions = df_Student_Submissions.withColumn("User_id_ques_id",concat(col("User_UID"), lit('-'), col("Question_ID")))
        df_Student_Submissions = df_Student_Submissions.withColumn('Qkey', 
            concat(col('Session'), col('Assessment_type'), col('Subject'), col('Grade'), col('Question_ID'))
        )     

        # Selecting required columns 
        fact_Student_Submissions = df_Student_Submissions.select(
            'User_UID','User_Role_ID','School_ID','Course_NID','Section_NID',
        'Section_Code','Item_ID','Item_Name','First_Access','Latest_Attempt','Total_Time','Submission_Grade','Submission','Question_ID',
        'Session','Assessment_type','Subject','Grade','Section','File_Name',
        'Position_Number','Sub-Question','Answer_Submission','Correct_Answer','Points_Received','Points_Possible','User_id_ques_id'
        )

        # 3 dim tables (dim_grade,dim_assessment_type, dim_subject) after student submissions fact table
        def generate_uuid(column1, column2):
            from pyspark.sql.functions import sha2, concat_ws
            combined_column = concat(column1, column2)
            uuid = sha2(combined_column, 256)  # Apply a hashing function to generate UUIDs
            return uuid
            
        fact_Student_Submissions = fact_Student_Submissions.withColumn("Grade_ID", generate_uuid(df_Student_Submissions['School_ID'], df_Student_Submissions['Grade']))
        fact_Student_Submissions = fact_Student_Submissions.withColumn("Assessment_ID", generate_uuid(df_Student_Submissions['School_ID'], df_Student_Submissions['Assessment_type']))
        fact_Student_Submissions = fact_Student_Submissions.withColumn("Subject_ID", generate_uuid(df_Student_Submissions['School_ID'], df_Student_Submissions['Subject']))

        self.publish(fact_Student_Submissions,f'stage2/Enriched/schoology/v{self.version}/fact_student_submission',f'stage3/Published/schoology/v{self.version}/fact_student_submission', primary_key='User_id_ques_id')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/fact_student_submission')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/fact_student_submission')


    
    def build_aggregation_tables(self):
        submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')   
        questiondata=oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
        standard= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard') 
        standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
        standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
        # agg for Total Student
        section= oea.load(f'stage3/Published/schoology/v{self.version}/dim_section')
        item= oea.load(f'stage3/Published/schoology/v{self.version}/dim_item')
        standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
        total_students=submissions.join(item,on=["Item_ID","School_ID"],how="left")
        total_students=total_students.join(section,on=["Section_NID","School_ID"])
        total_students=total_students.groupby("Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Section_NID","Section_Instructors","Session").agg(countDistinct("User_UID").alias("Total_Student"))
        total_students=total_students.na.drop(subset=["Assessment_type","School_ID","Grade","Subject","Section_NID","Session"])
        total_students = total_students.withColumn("ID", monotonically_increasing_id())
        self.publish(total_students,f'stage2/Enriched/schoology/v{schoology.version}/agg_Total_Student',f'stage3/Published/schoology/v{schoology.version}/agg_Total_Student', primary_key='ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Total_Student')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Total_Student')

        # agg Table for StandardByDeep_Dive
        questiondata=questiondata.join(standard,on=["Standards"])
        submissions=submissions.join(questiondata, on=["Question_ID","Assessment_type","School_ID","Grade","Subject","Item_Id","Session"], how="inner")
        submissions=submissions.join(item,on=["Item_ID","School_ID"])
        submissions=submissions.na.drop(subset=["Assessment_type","Section_NID","Session","School_ID","Grade","Subject"])
        standard1=submissions.select("Standards","School_ID","Grade","Subject","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime","subject")
        #calculation by standard
        calScorePossiblePoint=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Strand","Section_NID","Session").agg(round(sum("Points_Possible"),2).alias("Possible_PointsByStandard"),round(sum("Points_Received"),2).alias("Score_ByStandard"),countDistinct("Question_ID").alias("Total_Question_By_Standard"),countDistinct("Standards").alias("Total_Sub_Standard"))
        TotalScorebyStandard=calScorePossiblePoint.withColumn("% Total_Score_By_Standard", round((col("Score_ByStandard") / col("Possible_PointsByStandard")) * 100, 2))
        TotalScorebyStandard=TotalScorebyStandard.withColumn("% Total_Incorrect_Score_By_Standard", round(100 -col("% Total_Score_By_Standard"), 2))
        TotalScorebyStandard=TotalScorebyStandard.fillna({'Standards': 'Other'})
        calTotalUser=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Strand","Section_NID","Session").agg(countDistinct("User_UID").alias("Total_Student"))
        TotalScorebyStandard=TotalScorebyStandard.join(calTotalUser,on=["Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Strand","Section_NID","Session"])
        Selectedattribute = col("% Total_Score_By_Standard")
        High = 0.8
        Low = 0.7
        result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
        TotalScorebyStandard=TotalScorebyStandard.withColumn("Performance_Color_By_Standard", result)
        calScorePossiblePointByStrand=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Strand","Section_NID","Session").agg(round(sum("Points_Possible"),2).alias("Possible_PointsByStrand"),round(sum("Points_Received"),2).alias("Score_ByStrand"),countDistinct("Question_ID").alias("Total_Question_By_Strand"),countDistinct("Standards").alias("Total_Sub_Strand"))
        TotalScorebyStandardByStrand=calScorePossiblePointByStrand.withColumn("% Total_Score_By_Strand", round((col("Score_ByStrand") / col("Possible_PointsByStrand")) * 100, 2))
        TotalScorebyStandardByStrand=TotalScorebyStandardByStrand.withColumn("% Total_Incorrect_Score_By_Strand", round(100-col("% Total_Score_By_Strand"), 2))
        TotalScorebyStandardByStrand = TotalScorebyStandardByStrand.fillna({'Strand': 'Other'})
        Selectedattribute = col("% Total_Score_By_Strand") 
        result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
        TotalScorebyStandardByStrand=TotalScorebyStandardByStrand.withColumn("Performance_Color_By_Strand", result)

        FinalResult=TotalScorebyStandard.join(TotalScorebyStandardByStrand,on=["Assessment_type","School_ID","Grade","Subject","Item_Name","Strand","Section_NID","Session"])
        FinalResult=FinalResult.join(section,on=["Section_NID","School_ID"])
        FinalResult = FinalResult.withColumn("ID", monotonically_increasing_id())
        self.publish(FinalResult,f'stage2/Enriched/schoology/v{self.version}/agg_Standard_Deep_Dive',f'stage3/Published/schoology/v{self.version}/agg_Standard_Deep_Dive', primary_key='ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/agg_Standard_Deep_Dive')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/agg_Standard_Deep_Dive')
        #agg Table For agg_question_summary_report

        section =  oea.load(f'stage2/Enriched/schoology/v0.1/dim_section')
        question_df =  oea.load(f'stage2/Enriched/schoology/v0.1/dim_question_data')
        item_df =  oea.load(f'stage2/Enriched/schoology/v0.1/dim_item')
        submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
        student_df = oea.load(f'stage3/Published/schoology/v0.1/dim_student')
        student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
        submissions_df=submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Session"])
        student_df=student_df.withColumnRenamed('uid', 'User_UID')
        student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Session"])
        student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","School_ID"])
        submissions_df=submissions_df.join(item_df,on=["item_Id","School_ID"])
       # Question Based Calculation()
        DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Subject","Standards","Section_NID","Session","Points_Possible","Points_Received")
        CalDataByQuestion=DataByQuestion.groupby("Assessment_type","Question_No","School_ID","Item_Name","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
        CalDataByQuestion=CalDataByQuestion.withColumn("% Total_Score_By_Question", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
        finalDataByQuestion=CalDataByQuestion.join(DataByQuestion,on=["Assessment_type","Question_No","School_ID","Item_Name","Grade","Standards","Subject","Section_NID","Session"],how='inner')

        # Standard Based Calculation()
        Score=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Received").alias("Score"))
        Possible_Point=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("Possible Points"))
        Score=Score.join(Possible_Point,on=["Standards","Assessment_type","School_ID","Item_Name","Grade","Subject","User_UID","Section_NID","Session"],how="left")
        Standards=Score.groupBy("User_UID","School_ID","Item_Name","Standards","Subject","Assessment_type","Grade","Section_NID","Session").agg(sum("Possible Points").alias("Standard_Possible Points"))
        Received=Score.groupBy("User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Score").alias("Standard_Possible Receives"))
        Standards=Standards.join(Received,on=["User_UID","Item_Name","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session"])
        StandardsPercentage=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
        Standards=Standards.groupBy("School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session").agg(sum("Standard_Possible Points").alias("% Total_Possible_Point_By_Standard"),sum("Standard_Possible Receives").alias("% Total_Correct_Answer_By_Standard"))
        Standards=Standards.withColumn("% Total_Score_By_Standard", round((col("% Total_Correct_Answer_By_Standard") / col("% Total_Possible_Point_By_Standard")) * 100, 2))
        StandardsFinal=Standards.join(StandardsPercentage,on=["School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session"],how="inner")

        StandardsFinal=StandardsFinal.join(finalDataByQuestion,on=["Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session"],how="inner")

        User_UID=student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").alias("User_Possible_Points"))


        User_UID_Received=student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Received").alias("User_Possible_Received"))
        User_UID=User_UID.join(User_UID_Received,on=["User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Section_NID","Session"])
        User_UID=User_UID.withColumn("% User_Correct Answer", round((col("User_Possible_Received") / col("User_Possible_Points")) * 100, 2))

        Total_Possible_Points=User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Points").alias("Total_Possible_Point"))
        Total_Possible_Received=User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Received").alias("Total_Correct_Answer"))
        TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
        TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
        TotalTable_JoinFinal=TotalTable_Join.join(User_UID,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how='inner')
        TotalTable_JoinFinal=TotalTable_JoinFinal.join(StandardsFinal,on=["School_ID","Grade","User_UID","Item_Name","Subject","Assessment_type","Section_NID","Session"],how='inner')
        submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
        student_df = student_df.select("name_display","User_UID","School_ID")
        submissions_df=submissions_df.join(student_df,on=["User_UID","School_ID"])
        submissions_df=submissions_df.join(item_df,on=["Item_Id","Item_Name","School_ID"])
        submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Section_NID","Subject","Assessment_type","Item_Name","Grade","Session","name_display").agg(countDistinct("Item_Name").alias("Test_Taken"))
        submissions_Test_Taken_dfFinal=submissions_Test_Taken_df.join(TotalTable_JoinFinal,on=["User_UID","School_ID","Item_Name","Section_NID","Subject","Assessment_type","Grade","Session"],how='inner')
        submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.join(section,on=["School_ID","Section_NID"],how='inner')
        # submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
        # submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
        High = 0.8
        Low = 0.7
        Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column

        # Define the conditions and assign color values accordingly
        result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
        submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.withColumn("Performance_Color", result)
        Selectedattribute = col("% Standard_Correct Answer")
        submissions_df=submissions_Test_Taken_dfFinal.withColumn("Standard_Performance_Color", result)
        Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type").agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))

        submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type"])

        Selectedattribute = col("%_Total_Correct_Answer") 
        submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
        Selectedattribute = col("% Total_Score_By_Question")
        submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
        Selectedattribute = col("% Total_Score_By_Standard")
        submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
        Selectedattribute = col("% Teacher_Correct_Percentage")
        submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)
        submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())
        schoology.publish(submissions_df,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report',f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report', primary_key='ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report')

        #agg Table For agg_question_summary_report for Year To Date Report
 
        section =  oea.load(f'stage2/Enriched/schoology/v0.1/dim_section')
        question_df = oea.load(f'stage2/Enriched/schoology/v0.1/dim_question_data')
        item_df =  oea.load(f'stage2/Enriched/schoology/v0.1/dim_item')
        submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
        student_df = oea.load(f'stage3/Published/schoology/v0.1/dim_student')
        student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
        submissions_df=submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Session"])
        student_df=student_df.withColumnRenamed('uid', 'User_UID')
        student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Session"])
        student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","School_ID"])
        submissions_df=submissions_df.join(item_df,on=["item_Id","School_ID"])
        # Question Based Calculation()
        DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session","Points_Possible","Points_Received")
        CalDataByQuestion=DataByQuestion.groupby("Assessment_type","Question_No","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
        CalDataByQuestion=CalDataByQuestion.withColumn("% Total_Score_By_Question", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
        finalDataByQuestion=CalDataByQuestion.join(DataByQuestion,on=["Assessment_type","Question_No","School_ID","Grade","Standards","Subject","Section_NID","Session"],how='inner')

        # Item Based Calculation()
        DataByItem=student_submissions_df.select("Assessment_type","Item_Name","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session","Points_Possible","Points_Received")
        DataByItemJoin=student_submissions_df.select("Assessment_type","Item_Name","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session")
        UserCalDataByItem=DataByItem.groupby("Assessment_type","Item_Name","User_UID","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Item_user_Possible_Point"),sum("Points_Received").alias("% Item_user_Correct_Answer"))
        UserfinalDataByItem=UserCalDataByItem.withColumn("% Item_user_Score_By_Item", round((col("% Item_user_Correct_Answer") / col("% Item_user_Possible_Point")) * 100, 2))
        UserfinalDataByItem=UserCalDataByItem.join(UserfinalDataByItem,on=["Assessment_type","User_UID","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session","% Item_user_Correct_Answer","% Item_user_Possible_Point"],how='inner')
        CalDataByItem=DataByItem.groupby("Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Item"),sum("Points_Received").alias("% Total_Correct_Answer_By_Item"))
        finalDataByItem=CalDataByItem.withColumn("% Total_Score_By_Item", round((col("% Total_Correct_Answer_By_Item") / col("% Total_Possible_Point_By_Item")) * 100, 2))
        finalDataByItem=CalDataByItem.join(finalDataByItem,on=["Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session","% Total_Possible_Point_By_Item","% Total_Correct_Answer_By_Item"],how='inner')
        finalDataByItem=UserfinalDataByItem.join(finalDataByItem,on=["Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session"],how='inner')
        # Standard Based Calculation()
        Score=student_submissions_df.groupby("Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Received").alias("Score"))
        Possible_Point=student_submissions_df.groupby("Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("Possible Points"))
        Score=Score.join(Possible_Point,on=["Standards","Assessment_type","School_ID","Grade","Subject","User_UID","Section_NID","Session"],how="left")
        Standards=Score.groupBy("User_UID","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session").agg(sum("Possible Points").alias("Standard_Possible Points"))
        Received=Score.groupBy("User_UID","School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Score").alias("Standard_Possible Receives"))
        Standards=Standards.join(Received,on=["User_UID","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session"])
        StandardsPercentage=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
        Standards=Standards.groupBy("School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Standard_Possible Points").alias("% Total_Possible_Point_By_Standard"),sum("Standard_Possible Receives").alias("% Total_Correct_Answer_By_Standard"))
        Standards=Standards.withColumn("% Total_Score_By_Standard", round((col("% Total_Correct_Answer_By_Standard") / col("% Total_Possible_Point_By_Standard")) * 100, 2))
        StandardsFinal=Standards.join(StandardsPercentage,on=["School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session"],how="inner")

        StandardsFinal=StandardsFinal.join(finalDataByQuestion,on=["Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session"],how="inner")
        StandardsFinal=StandardsFinal.join(finalDataByItem,on=["Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session"],how="inner")

        User_UID=student_submissions_df.groupBy("User_UID","School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").alias("User_Possible_Points"))


        User_UID_Received=student_submissions_df.groupBy("User_UID","School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Received").alias("User_Possible_Received"))
        User_UID=User_UID.join(User_UID_Received,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Section_NID","Session"])
        User_UID=User_UID.withColumn("% User_Correct Answer", round((col("User_Possible_Received") / col("User_Possible_Points")) * 100, 2))

        Total_Possible_Points=User_UID.groupBy("School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Points").alias("Total_Possible_Point"))
        Total_Possible_Received=User_UID.groupBy("School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Received").alias("Total_Correct_Answer"))
        TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
        TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
        TotalTable_JoinFinal=TotalTable_Join.join(User_UID,on=["School_ID","Grade","Subject","Assessment_type","Section_NID","Session"],how='inner')
        TotalTable_JoinFinal=TotalTable_JoinFinal.join(StandardsFinal,on=["School_ID","Grade","User_UID","Subject","Assessment_type","Section_NID","Session"],how='inner')

        submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
        student_df = student_df.select("name_display","User_UID","School_ID")
        submissions_df=submissions_df.join(student_df,on=["User_UID","School_ID"])
        submissions_df=submissions_df.join(item_df,on=["Item_Id","Item_Name","School_ID"])
        submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Section_NID","Subject","Assessment_type","Grade","Session","name_display").agg(countDistinct("Item_Name").alias("Test_Taken"))
        submissions_Test_Taken_dfFinal=submissions_Test_Taken_df.join(TotalTable_JoinFinal,on=["User_UID","School_ID","Section_NID","Subject","Assessment_type","Grade","Session"],how='inner')
        submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.join(section,on=["School_ID","Section_NID"],how='inner')
        # submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
        # submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
        High = 0.8
        Low = 0.7
        Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column

        # Define the conditions and assign color values accordingly
        result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
        submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.withColumn("Performance_Color", result)
        Selectedattribute = col("% Standard_Correct Answer")
        submissions_df=submissions_Test_Taken_dfFinal.withColumn("Standard_Performance_Color", result)
        Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Section_Instructors","Grade","Subject","Assessment_type").agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))

        submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Section_Instructors","Grade","Subject","Assessment_type"])

        Selectedattribute = col("%_Total_Correct_Answer") 
        submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
        Selectedattribute = col("% Total_Score_By_Question")
        submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
        Selectedattribute = col("% Total_Score_By_Item")
        submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Item", result)
        Selectedattribute = col("% Total_Score_By_Standard")
        submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
        Selectedattribute = col("% Teacher_Correct_Percentage")
        submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)
        submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())
        self.publish(submissions_df,f'stage2/Enriched/schoology/v{self.version}/agg_question_summary_report_longer',f'stage3/Published/schoology/v{self.version}/agg_question_summary_report_longer', primary_key='ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/agg_question_summary_report_longer')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/agg_question_summary_report_longer')

        # agg_Grade_Performance_by_Strand
        student_submissions_df = oea.load(f'stage2/Enriched/schoology/v{self.version}/fact_student_submission')
        standards_df =  oea.load(f'stage2/Enriched/schoology/v{self.version}/dim_standard')
        question_df =  oea.load(f'stage2/Enriched/schoology/v{self.version}/dim_question_data')
        item = oea.load(f'stage2/Enriched/schoology/v{self.version}/dim_item')
        joined_df = student_submissions_df.join(question_df[['Question_ID','Standards']], 'Question_ID', 'inner')
        joined_df = joined_df.join(standards_df[['Schoology_Standard','Strand']], joined_df['Standards'] == standards_df['Schoology_Standard'], 'inner')
        joined_df = joined_df.select('Question_ID','Schoology_Standard','Strand')
        student_submissions_df = student_submissions_df.join(joined_df,student_submissions_df['Question_ID'] == joined_df['Question_ID'])

        grouped_df = student_submissions_df.groupBy('Item_ID', 'Subject', 'Grade','School_ID','Strand').agg(sum('Points_Received').alias('Total_Score'), sum('Points_Possible').alias('Total_Possible_Points'))
        grouped_df = grouped_df.withColumn('Percentage_Correct_Answers', col('Total_Score') / col('Total_Possible_Points') * 100)
        grouped_df = grouped_df.withColumn('Percentage_Correct_Answers', round(grouped_df['Percentage_Correct_Answers'], 2))
        grouped_df= grouped_df.join(item[['Item_ID','Item_Name']], grouped_df['Item_ID']== item['Item_ID'])
        grouped_df = grouped_df.drop(item['Item_ID'])

        grouped_df = grouped_df.withColumn("ID", monotonically_increasing_id())
        grouped_df = grouped_df.dropna(subset=['Subject', 'Grade'])
        self.publish(grouped_df,f'stage2/Enriched/schoology/v{self.version}/agg_Grade_Performance_by_Strand',f'stage3/Published/schoology/v{self.version}/agg_Grade_Performance_by_Strand', primary_key='ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/agg_Grade_Performance_by_Strand')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/agg_Grade_Performance_by_Strand')

       # agg_ques_resp_analysis
        standards_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_standard')
        student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')  
        question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
        dim_item = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
        section_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
        student_submissions_df = student_submissions_df.join(section_df[["Section_Instructors","Section_NID"]],on="Section_NID")

        Score = student_submissions_df.groupby("Assessment_type","Session" ,"School_ID","Section_Instructors" ,"Section_NID", "Section_Instructors","Grade", "Subject", "Item_Name", "Question_No", "Answer_Submission").agg(round(sum("Points_Received"), 2).alias("Score"),
            sum("Points_Possible").alias("Possible_Points"))

        total_points_table = student_submissions_df.groupby(
            "Assessment_type","Session" ,"School_ID", "Grade", "Subject","Section_NID", "Section_Instructors", "Item_Name", "Question_No").agg(sum("Points_Possible").alias("Total_points"))
        joined_df = Score.join(
            total_points_table,
            ["Assessment_type","Session","School_ID", "Grade", "Subject","Section_NID", "Section_Instructors", "Item_Name", "Question_No"],
            "inner")
        percentage_correct_df = joined_df.groupBy("Assessment_type","Session","School_ID", "Grade", "Section_NID", "Section_Instructors","Subject","Item_Name", "Question_No").agg(
            sum("Score").alias("Total_Score"),
            sum("Possible_Points").alias("Total_Possible_Points"))

        percentage_correct_df = percentage_correct_df.withColumn(
            "Percentage_Correct_Answers",
                round((col("Total_Score") / col("Total_Possible_Points")) *100 ,1)
        )
        Selectedattribute = col("Percentage_Correct_Answers")  # Assuming you have already calculated the Score_Percentage column
        color_result = (
                when(Selectedattribute/100 <.7,"#FFCCFF") \
                .when(Selectedattribute/100 >.8,"LightGreen") \
                .when(Selectedattribute/100 >= .7,"Yellow") \
                .otherwise(None)
                )
        percentage_correct_df = percentage_correct_df.withColumn("Performance_Color", color_result)
        incorrect_submissions = joined_df.filter(col("Score") == 0)
        incorrect_perc = incorrect_submissions.withColumn("Perc", col("Possible_Points") / col("Total_points") * 100)
        results = incorrect_perc.withColumn("Result", concat(
            format_string("%.2f%%", col("Perc")), lit(" chose ["), col("Answer_Submission"), lit("]")))

        grouped_result = results.groupBy("Assessment_type","Session","School_ID", "Section_NID", "Section_Instructors","Grade", "Subject", "Item_Name", "Question_No") \
                                .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
        final_result = grouped_result.join(
            percentage_correct_df,
            ["Assessment_type","Session","School_ID", "Grade", "Subject","Item_Name", "Question_No"],
            "inner")

        grouped_question_df = question_df.groupBy("Question_No","Question_ID","Correct_Answer", "Question","Standards").agg(
            concat_ws(", ", col("Standards")).alias("all_Standards")
        )
        grouped_question_df = grouped_question_df.drop("Standards")
        # Rename the "all_Standards" column to "Standards"
        grouped_question_df = grouped_question_df.withColumnRenamed("all_Standards", "Standards")
        ques_stand_df = final_result.join(grouped_question_df, "Question_ID", "inner")
        # Drop one of the "Question_ID" columns
        ques_stand_df = ques_stand_df.drop(grouped_question_df["Question_ID"]) 
        ques_stand_desc = ques_stand_df.join(
            standards_df.select("Schoology_Standard", "description","Strand"),
            ques_stand_df["Standards"] == standards_df["Schoology_Standard"],
            "left"
        )
        ques_stand_desc = ques_stand_desc.drop("Schoology_Standard")
        ques_stand_desc = ques_stand_desc.fillna({'Strand': 'Other'})
        ques_stand_desc = ques_stand_desc.fillna({'Standards': 'Other'})
        ques_stand_desc = ques_stand_desc.fillna({'description': 'Other'})
        ques_stand_desc = ques_stand_desc.join(dim_item[["Item_ID","Item_Name"]],on='Item_ID')
        ques_stand_desc = ques_stand_desc.withColumn("unique_id", concat(col("School_ID"), col("Grade"), col("Item_Name"),col("Session"), col("Subject"), col("Strand")))
        ques_stand_desc = ques_stand_desc.withColumn("Incorrect_Choice_details", expr("SUBSTRING(Incorrect_Choice_details, 1, 8000)"))
        ques_stand_desc= ques_stand_desc.fillna({'Strand': 'Other'})
        ques_stand_desc= ques_stand_desc.fillna({'Standards': 'Other'})
        ques_stand_desc = ques_stand_desc.drop("opt_students_attempted", "Ques_total_Students")


        # Adding assesment_date
        student_submissions_df = student_submissions_df.withColumn(
            'assessment_attempt', 
            date_format(col('Latest_Attempt'), 'dd/MM/yyyy')
        )
        student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject","Section_NID", "Section_Instructors", "Item_ID","assessment_attempt"]].dropDuplicates()
        concatenated_df = student_submissions_date.groupby(
            "Assessment_type", "Session", "School_ID", "Grade","Section_NID", "Section_Instructors", "Subject", "Item_ID"
        ).agg(
            concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date")
        )
        ques_stand_desc_attempt = ques_stand_desc.join(
            concatenated_df,
            on=["Assessment_type", "Session", "School_ID", "Grade","Section_NID", "Section_Instructors", "Subject", "Item_ID"],
            how="inner"
        )
        ques_stand_desc_attempt = ques_stand_desc_attempt.withColumn("ID", monotonically_increasing_id())
        # filtered_df = ques_stand_desc_attempt.filter(
        #     (col("Assessment_type") == 'Unit') &
        #     (col("Session") == '2023-24') &
        #     # (col("Grade") == 'Grade 3') &
        #     (col("Question_ID") == '1862157938')
        # )
        # filtered_df.show(1)
        self.publish(ques_stand_desc_secID_inst,f'stage2/Enriched/schoology/v{self.version}/agg_question_response_analysis',f'stage3/Published/schoology/v{self.version}/agg_question_response_analysis', primary_key='ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/agg_question_response_analysis')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/agg_question_response_analysis')

        # Submission_Summary published as aggregation table
        df_Submission_Summary = oea.load(f'stage2/Ingested/schoology/v{self.version}/submission_summary')
        self.publish(df_Submission_Summary,f'stage2/Enriched/schoology/v{self.version}/agg_Submission_Summary',f'stage3/Published/schoology/v{self.version}/agg_Submission_Summary', primary_key='Unique_Key')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/agg_Submission_Summary')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/agg_Submission_Summary')

        # agg_Incorrect_Details
        student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')  
        dim_item = oea.load(f'stage2/Enriched/schoology/v{self.version}/dim_item')

        all_student_submissions_df = student_submissions_df[['User_UID', 'School_ID', 'Item_ID','Question_ID', 'Session', 'Assessment_type', 'Subject', 'Grade', 'Section', 'Answer_Submission', 'Correct_Answer', 'Points_Received', 'Points_Possible']]
        total_possible_df = all_student_submissions_df.groupby(
            'School_ID', 'Assessment_type','Item_ID', 'Question_ID', 'Session','Subject', 'Grade','Correct_Answer'
        ).agg(
            F.sum('Points_Possible').alias('Total_Possible_Points')
        )
        incorrect_submissions = all_student_submissions_df[student_submissions_df['Points_Received'] != student_submissions_df['Points_Possible']]
        incorrect_submissions_df = incorrect_submissions[['User_UID', 'School_ID', 'Assessment_type','Item_ID', 'Question_ID', 'Session', 'Subject', 'Grade', 'Answer_Submission', 'Correct_Answer', 'Points_Received', 'Points_Possible']]
        incorrect_choices_df = incorrect_submissions_df.groupby(
            'School_ID', 'Item_ID', 'Question_ID', 'Session', 'Assessment_type','Subject', 'Grade', 'Points_Possible',"Correct_Answer"
        ).agg(
            F.countDistinct('Answer_Submission').alias('Total_Incorrect_Choices'),
        )
        incorrect_students_df = incorrect_submissions_df.groupby(
            'School_ID', 'Item_ID', 'Question_ID', 'Session', 'Assessment_type','Subject', 'Grade', 'Answer_Submission','Points_Possible',"Correct_Answer"
        ).agg(
            F.countDistinct('User_UID').alias('Total_Incorrect_Students'),
        )
        incorrect_students_points_possible = incorrect_students_df.join(total_possible_df,on=['School_ID', 'Item_ID','Assessment_type', 'Question_ID', 'Session', 'Subject', 'Grade',"Correct_Answer"], how="inner")
        incorrect_students_df_perc = incorrect_students_points_possible.withColumn('Perc_incorrect_answers', F.round((F.col('Total_Incorrect_Students') * F.col('Points_Possible')) / F.col('Total_Possible_Points') * 100, 2))
        incorrect_students_df_perc = incorrect_students_df_perc.withColumn('Perc_incorrect_answers', F.concat(F.col('Perc_incorrect_answers'), F.lit('%')))
        incorrect_students_df_perc_item = incorrect_students_df_perc.join(dim_item[["Item_ID","Item_Name"]],on='Item_ID')
        incorrect_students_df_perc_item = incorrect_students_df_perc_item.withColumn("ID", monotonically_increasing_id())
        self.publish(incorrect_students_df_perc_item,f'stage2/Enriched/schoology/v{self.version}/agg_incorrect_answer_details',f'stage3/Published/schoology/v{self.version}/agg_incorrect_answer_details', primary_key='ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/agg_incorrect_answer_details')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/agg_incorrect_answer_details')
        # table - 2 (agg_Incorrect_Details - 2)
        # Student Name and his incorrect answer submission
        student_names = oea.load(f'stage3/Published/schoology/v0.1/agg_question_summary_report')  
        student_names = student_names[["name_display","User_UID"]].dropDuplicates(["name_display","User_UID"])
        incorrect_submissions_names = incorrect_submissions.join(student_names["name_display","User_UID"],on="User_UID",how="inner")
        incorrect_submissions_names = incorrect_submissions_names.join(dim_item[["Item_ID","Item_Name"]],on='Item_ID')

        incorrect_submissions_names = incorrect_submissions_names.withColumn("ID", monotonically_increasing_id())
        incorrect_submissions_names = incorrect_submissions_names.drop("Points_Received", "Points_Possible")
        self.publish(incorrect_submissions_names,f'stage2/Enriched/schoology/v{self.version}/agg_incorrect_answer_details2',f'stage3/Published/schoology/v{self.version}/agg_incorrect_answer_details2', primary_key='ID')
        oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/agg_incorrect_answer_details2')
        oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/agg_incorrect_answer_details2')

schoology = Schoology()


# In[ ]:





# In[ ]:


# Testing code for aggregate tables


# In[300]:


# Code of all aggregates 
# submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
# questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
# standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
# section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
# item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
# standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
# standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
# # agg for Total Student
# total_students = submissions.join(item,on=["Item_ID","Item_Name","School_ID"],how="left")
# total_students = total_students.join(section,on=["Section_NID","School_ID"])
# total_students = total_students.groupby("Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Section_NID","Section_Instructors","Session").agg(countDistinct("User_UID").alias("Total_Student"))
# total_students = total_students.na.drop(subset=["Assessment_type","School_ID","Grade","Subject","Section_NID","Session"])
# total_students = total_students.withColumn("ID", monotonically_increasing_id())
# schoology.publish(total_students,f'stage2/Enriched/schoology/v{schoology.version}/agg_Total_Student',f'stage3/Published/schoology/v{schoology.version}/agg_Total_Student', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Total_Student')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Total_Student')

# Suruchi Standard deep dive for accurate results
# standard deep dive -row 1 
# Strand wise calculation() student
submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data') 
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
# standard = standard.select("Standards","cPalms_Standard","Strand")
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
distinct_questiondata = questiondata.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID', 'Standards'])
distinct_questiondata = distinct_questiondata.select('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID', 'Standards')
questiondata_standards = distinct_questiondata.join(standard[["Standards", "cPalms_Standard", "Strand"]],on=["Standards"],how="left")
submissions = submissions.dropDuplicates(['User_UID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Section_NID','Points_Received','Points_Possible'])
submissions = submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Submission_Grade','Section_NID','Points_Received','Points_Possible')
all_submissions = submissions.join(questiondata_standards,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID'])
#strand wise calculation()
strand_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'Strand').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers_by_strand'),
    countDistinct(col('Question_ID')).alias('strandwise_wise_Total_Questions'),
    countDistinct(col('Standards')).alias('strandwise_Total_Standards'),
    countDistinct(col('User_UID')).alias('Total_Students')
)
# percentage sign ahead
# strand_wise = strand_wise.withColumn('Percentage_Correct_Answers_by_strand',
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# ) 
# strand_wise = strand_wise.drop("Percentage_Correct_Answers")
#teacherwise  Calculation()
filters_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('grade_avg'),
    countDistinct(col('Question_ID')).alias('Full_Total_Questions'),
    countDistinct(col('Standards')).alias('Full_Total_Standards'),
    countDistinct(col('User_UID')).alias('Full_Total_Students')
)
# percentage sign ahead
# filters_wise = filters_wise.withColumn('grade_avg_perc',
#     concat(col('grade_avg').cast("string"), lit('%'))
# )
# filters_wise = filters_wise.drop("Percentage_Correct_Answers")
# joining strandwise and full table
final = filters_wise.join(strand_wise,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
Selectedattribute = col("Percentage_Correct_Answers_by_strand")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
final = final.fillna({'Strand': 'Other'})
final = final.withColumn("Performance_Color_By_strand", result)
final = final.drop("grade_avg")
# standard deep dive -row 2 
#Standard wise calculation()
standard_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'Standards').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers_by_Standard'),
    countDistinct(col('Question_ID')).alias('standard_wise_wise_Total_Questions'),
    countDistinct(col('Standards')).alias('standard_wise_wise_Total_SubStandards'),
)
Selectedattribute = col("Percentage_Correct_Answers_by_Standard")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
standard_wise = standard_wise.withColumn("Performance_Color_By_Standard", result)
# percentage sign ahead
# standard_wise = standard_wise.withColumn('Percentage_Correct_Answers_by_Standard',
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# ) 
# standard_wise = standard_wise.drop("Percentage_Correct_Answers")
# standard deep dive -row 2 last_table
# Short_Standard wise calculation()
short_standard_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard').agg(
    sum(col('Points_Possible')).alias('Total_points_by_short_standrd'),
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers_by_short_Standard'),
    countDistinct(col('Question_ID')).alias('short_standard_wise_Total_Questions'),
)
# percentage sign ahead
# short_standard_wise = short_standard_wise.withColumn('Percentage_Correct_Answers_by_short_Standard',
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# ) 
# short_standard_wise = short_standard_wise.drop("Percentage_Correct_Answers")
short_standard_wise = short_standard_wise.fillna({'cPalms_Standard': 'Other'})
# % of incorrect choices
incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]
Incorrect_Choices = incorrect_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard').agg(
    sum(col('Points_Possible')).alias('wrong_points_by_short_standard'))
short_standard_wise = short_standard_wise.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard'],how="left")
short_standard_wise_table = short_standard_wise.withColumn('Incorrect_Choices_Percentage_by_short_standard',
        round((col('wrong_points_by_short_standard') / col('Total_points_by_short_standrd')) * 100, 2)
)
# join three tables
# standard deep dive -row 2 
#Standard wise calculation()
joined = final.join(standard_wise,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
FinalResult = joined.join(short_standard_wise_table,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
#getting section_Instructors
FinalResult = FinalResult.join(section[["Section_NID","Section_Instructors"]],on="Section_NID")
# to get description column for hovering
FinalResult = FinalResult.join(standard[["Standards","description"]],on=["Standards"],how="left")
# FinalResult.filter(col("Item_ID")=='6772003024').show(100)
FinalResult = FinalResult.withColumn("UniqueID",concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Standards"), col("Session")))
FinalResult = FinalResult.withColumn("ID", monotonically_increasing_id())

# schoology.publish(FinalResult,f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive',f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive')


# # agg_question_response_analysis
# # But i will do changes here for best results
# section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
# question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
# item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# # submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
# student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
# standard= oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
# standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
# standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
# # question_df = question_df.join(standard,on=["Standards"],how="left")
# distinct_questiondata = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# # distinct_questiondata = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Question_No','Question',\
# # 'Position_Number','Correct_Answer','Standards'])
# distinct_questiondata = distinct_questiondata.select('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Position_Number')
# submissions = student_submissions_df.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID',\
# 'Position_Number','Answer_Submission','Points_Received','Points_Possible'])
# submissions = submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade',\
# 'Section_NID','Question_ID','Position_Number','Answer_Submission','Points_Received','Points_Possible')
# submissions_questions = submissions.join(distinct_questiondata,on=['Session', 'School_ID','Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# # student_submissions_df = student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
# all_submissions = submissions_questions.join(item_df,on=["item_ID","Item_Name","School_ID"])
# perc_correct_ans = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number').agg(\
#     sum(col('Points_Possible')).alias('Total_points'),
#     round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers')
# )
# Selectedattribute = col("Percentage_Correct_Answers")
# High = 0.8
# Low = 0.7
# result = (
# when(Selectedattribute/100 >= High, "#99FF99") \
#     .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
#     .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
#     .otherwise(None)
# )
# perc_correct_ans = perc_correct_ans.withColumn("Performance_Color_By_Question", result)
# perc_correct_ans = perc_correct_ans.withColumn('Percentage_of_Correct_Answers',               # % age sign
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# )
# perc_correct_ans = perc_correct_ans.drop("Percentage_Correct_Answers") 
# # Incorrect Choice Details
# incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]
# # incorrect_submissions = incorrect_submissions.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# # 'Points_Received','Points_Possible','Answer_Submission'])
# # incorrect_submissions = incorrect_submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# # 'Points_Received','Points_Possible','Answer_Submission')
# Incorrect_Choices = incorrect_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number','Answer_Submission').agg(
#     sum(col('Points_Possible')).alias('wrong_points'))
# incorrect_option_perc = perc_correct_ans.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number'],how="left")
# incorrect_option_perc = incorrect_option_perc.withColumn('Incorrect_Choices_Percentage',
#     concat(
#         round((col('wrong_points') / col('Total_points')) * 100, 2).cast("string"),
#         lit('%')
#     )
# )
# results = incorrect_option_perc.withColumn("Result", concat(col("Incorrect_Choices_Percentage"), lit(" chose ["), col("Answer_Submission"), lit("]")))
# grouped_result = results.groupBy('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number','Percentage_of_Correct_Answers','Performance_Color_By_Question') \
#                 .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
# grouped_result = grouped_result.orderBy('Position_Number')
# student_submissions_attempt = student_submissions_df.withColumn(
# 'assessment_attempt',
# date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
# student_submissions_date = student_submissions_attempt[['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID',"assessment_attempt"]].dropDuplicates()
# concatenated_df = student_submissions_date.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID')\
# .agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
# final_attempt = grouped_result.join(concatenated_df,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID'],how='inner')
# final_attempt_ins = final_attempt.join(section[["Section_Instructors","Section_NID","School_ID"]],on=["Section_NID","School_ID"])
# final_attempt_ins = final_attempt_ins.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
# # join with standarsd to get standard and description
# distinct_questiondata_info = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number','Question_No','Question',"Standards"])
# question_stand = distinct_questiondata_info.join(standard[["Standards","Description"]],on=["Standards"],how="left")
# question_stand = question_stand.fillna({'Standards': 'Others', 'Description': 'Others'})
# final_attempt_ins = final_attempt_ins.join(question_stand,on=['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# final_attempt_ins = final_attempt_ins.withColumn(
# "UniqueID",
# concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Standards"), col("Session")))
# final_attempt_ins = final_attempt_ins.withColumn("ID", monotonically_increasing_id())
# schoology.publish(final_attempt_ins,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis',\
# f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis')


# agg_incorrect_answer_details table 
# student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')  
# dim_item = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# ques_data =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
# dim_section = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
# student_submissions_df =student_submissions_df.join(dim_section[["Section_Instructors","Section_NID"]],on="Section_NID")
# all_student_submissions_df = student_submissions_df[['User_UID', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Session', 'Assessment_type', 'Subject', 'Grade', 'Section_NID',\
# 'Section_Instructors', 'Answer_Submission','Points_Received', 'Points_Possible']].dropDuplicates()

# all_student_submissions_df = all_student_submissions_df.join(ques_data[["Question_ID","Question_No","Correct_Answer"]].dropDuplicates(),on=["Question_ID"])
# total_possible_df = all_student_submissions_df.groupby(
#     'Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade', 'Correct_Answer'
# ).agg(
#     F.sum('Points_Possible').alias('Total_Possible_Points')
# )
# incorrect_submissions = all_student_submissions_df[all_student_submissions_df['Points_Received'] < all_student_submissions_df['Points_Possible']]
# incorrect_submissions_df = incorrect_submissions[['User_UID',  'Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', \
# 'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission', 'Points_Received', 'Points_Possible']]
# should_possible =  incorrect_submissions_df.groupby('Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', \
# 'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission').agg(
#     F.sum('Points_Possible').alias('should_Possible_Points')
# )
# perc = total_possible_df.join(should_possible,on=['Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', \
# 'Section_Instructors', 'Grade', 'Correct_Answer'],how="left")
# perc = perc.withColumn('Percentage_incorrect_options',
#     round((col('should_Possible_Points') / col('Total_Possible_Points')) * 100, 2)
# )
# perc = perc.withColumn('Percentage_with_sign',
#     concat(col('Percentage_incorrect_options').cast("string"), lit('%'))
# )
# perc = perc.drop("should_Possible_Points","Total_Possible_Points","Percentage_incorrect_options")
# incorrect_students_df = incorrect_submissions_df.groupby(
#     'Session', 'School_ID', 'Item_ID', 'Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission'
# ).agg(
#     F.count('User_UID').alias('Number_of_students'),
# )
# students_percentage_both = perc.join(incorrect_students_df,on=['Session', 'School_ID', 'Item_ID', 'Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade',\
# 'Correct_Answer', 'Answer_Submission'],how="left")
# incorrect_choices_df = incorrect_submissions_df.groupby(
#      'Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade', 'Correct_Answer'
# ).agg(
#     F.countDistinct('Answer_Submission').alias('Total_Incorrect_Choices'),
# )
# incorrect_table = students_percentage_both.join(incorrect_choices_df,on=['Session', 'School_ID', 'Item_ID', 'Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors',\
# 'Grade', 'Correct_Answer','Item_Name'],how="left")
# incorrect_table = incorrect_table.withColumn("ID", monotonically_increasing_id())
# schoology.publish(incorrect_table,f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details',f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details')
# # table - 2 (agg_Incorrect_Details - 2)
# # Student Name and his incorrect answer submission
# student_names = oea.load(f'stage2/Ingested/schoology/v0.1/student_submissions')  
# student_names = student_names[["First_Name","Last_Name","User_UID"]].dropDuplicates()
# student_names = student_names.withColumn('Student_Name', concat_ws(' ', col('First_Name'), col('Last_Name')))
# incorrect_submissions = all_student_submissions_df[all_student_submissions_df['Points_Received'] < all_student_submissions_df['Points_Possible']]
# incorrect_submissions_df = incorrect_submissions[['User_UID',  'Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', \
# 'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission', 'Points_Received', 'Points_Possible']]
# incorrect_submissions_names = incorrect_submissions_df.join(student_names["Student_Name","User_UID"],on="User_UID",how="left")
# incorrect_submissions_names = incorrect_submissions_names.drop("Points_Received", "Points_Possible")
# incorrect_submissions_names = incorrect_submissions_names.withColumn("ID", monotonically_increasing_id())
# schoology.publish(incorrect_submissions_names,f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details2',f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details2', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details2')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details2')


# # agg_question_summary_report 
# section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
# question_df= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')
# item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
# student_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_student')
# student_df =  student_df.withColumnRenamed('uid', 'User_UID')
# student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
# # deduplicatedquestion_df = question_df[["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session","Question_No"]].dropDuplicates()
# # student_submissions_df = student_submissions_df.join(deduplicatedquestion_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
# deduplicated_question_df = question_df.groupBy(
#     "Question_Id", "Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Item_Name", "Session", "Question_No"
# ).agg(
#     first("Standards").alias("Standards")      # fetching one question of any standard
# )
# student_submissions_df = student_submissions_df.join(
#     deduplicated_question_df,
#     on=["Question_Id", "Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Item_Name", "Session"]
# )
# student_submissions_df = student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
# student_submissions_df = student_submissions_df.withColumn("Points_Received_by_one",col("Points_Received")/col("Points_Possible"))
# student_submissions_df = student_submissions_df.drop("Points_Received")
# student_submissions_df = student_submissions_df.withColumnRenamed("Points_Received_by_one","Points_Received")
# student_submissions_df = student_submissions_df.withColumnRenamed("Points_Received_by_one","Points_Received")
# # Question Based Calculation()
# DataByQuestion = student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Subject","Standards","Section_NID","Session",\
# "Points_Possible","Points_Received")
# CalDataByQuestion = DataByQuestion.groupby("User_UID","Assessment_type","Question_No","School_ID","Item_Name","Grade","Standards","Subject","Section_NID","Session").\
# agg(sum("Points_Possible").\
# alias("% Total_Possible_Point_By_Question")\
# ,sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
# CalDataByQuestion = CalDataByQuestion.withColumn("% Total_Score_By_Question", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
# finalDataByQuestion = CalDataByQuestion.join(DataByQuestion,on=["Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Standards","Subject","Section_NID",\
# "Session"],how='inner')
# # Standard Based Calculation()
# Score=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Received")\
# .alias("Score"))
# Possible_Point=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").\
# agg(sum("Points_Possible").alias("Possible Points"))
# Score=Score.join(Possible_Point,on=["Standards","Assessment_type","School_ID","Item_Name","Grade","Subject","User_UID","Section_NID","Session"],how="left")
# Standards=Score.groupBy("User_UID","School_ID","Item_Name","Standards","Subject","Assessment_type","Grade","Section_NID","Session").agg(sum("Possible Points").
# alias("Standard_Possible Points"))
# Received=Score.groupBy("User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Score").\
# alias("Standard_Possible Receives"))
# Standards=Standards.join(Received,on=["User_UID","Item_Name","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session"])
# StandardsPercentage=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
# Standards=Standards.groupBy("School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session").agg(sum("Standard_Possible Points").alias("% Total_Possible_Point_By_Standard"),\
# sum("Standard_Possible Receives").alias("% Total_Correct_Answer_By_Standard"))
# Standards = Standards.withColumn("% Total_Score_By_Standard", round((col("% Total_Correct_Answer_By_Standard") / col("% Total_Possible_Point_By_Standard")) * 100, 2))
# StandardsFinal = Standards.join(StandardsPercentage,on=["School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session"],how="inner")
# StandardsFinal=StandardsFinal.join(finalDataByQuestion,on=["Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session"],how="inner")
# # User_UID = student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").\
# # alias("User_Possible_Points"))
# User_UID = student_submissions_df.groupBy(
#     "User_UID", "School_ID", "Item_Name", "Grade", "Subject", "Assessment_type", "Section_NID", "Session"
# ).agg(
#     countDistinct("Question_ID").alias("User_Possible_Points")
# )
# User_UID_Received = student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").\
# agg(sum("Points_Received").alias("User_Possible_Received"))
# User_UID = User_UID.join(User_UID_Received,on=["User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Section_NID","Session"])
# User_UID = User_UID.withColumn("% User_Correct Answer", round((col("User_Possible_Received") / col("User_Possible_Points")) * 100, 2))
# Total_Possible_Points = User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Points").\
# alias("Total_Possible_Point"))
# Total_Possible_Received=User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Received").\
# alias("Total_Correct_Answer"))
# TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
# TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
# TotalTable_JoinFinal=TotalTable_Join.join(User_UID,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how='inner')
# TotalTable_JoinFinal=TotalTable_JoinFinal.join(StandardsFinal,on=["School_ID","Grade","User_UID","Item_Name","Subject","Assessment_type","Section_NID","Session"],how='inner')
# submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
# student_df = student_df.select("name_display","User_UID")
# submissions_df=submissions_df.join(student_df,on=["User_UID"])
# submissions_df=submissions_df.join(item_df,on=["Item_Id","Item_Name","School_ID"])
# submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Section_NID","Subject","Assessment_type","Item_Name","Grade","Session","name_display").\
# agg(countDistinct("Item_Name").alias("Test_Taken"))
# submissions_Test_Taken_dfFinal=submissions_Test_Taken_df.join(TotalTable_JoinFinal,on=["User_UID","School_ID","Item_Name","Section_NID","Subject","Assessment_type","Grade",\
# "Session"],how='inner')
# submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.join(section,on=["School_ID","Section_NID"],how='inner')
# # submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
# # submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
# High = 0.8
# Low = 0.7
# Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column
# # Define the conditions and assign color values accordingly
# result = (
# when(Selectedattribute/100 >= High, "#99FF99") \
# .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
# .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
# .otherwise(None)
# )
# submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.withColumn("Performance_Color", result)
# Selectedattribute = col("% Standard_Correct Answer")
# submissions_df=submissions_Test_Taken_dfFinal.withColumn("Standard_Performance_Color", result)
# Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type").\
# agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))
# submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type"])
# Selectedattribute = col("%_Total_Correct_Answer")
# submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
# Selectedattribute = col("% Total_Score_By_Question")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
# Selectedattribute = col("% Total_Score_By_Standard")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
# Selectedattribute = col("% Teacher_Correct_Percentage")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)
# submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())
# submissions_df = submissions_df.withColumn("Points_Possible", lit("1"))
# schoology.publish(submissions_df,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report',f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report')


# #agg Table For agg_question_summary_report for Year To Date Report
# section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
# question_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
# item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
# student_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_student')
# student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
# submissions_df=submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
# student_df=student_df.withColumnRenamed('uid', 'User_UID')
# student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
# student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
# submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
# # Question Based Calculation()
# DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session","Points_Possible","Points_Received")
# CalDataByQuestion=DataByQuestion.groupby("Assessment_type","Question_No","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
# CalDataByQuestion=CalDataByQuestion.withColumn("% Total_Score_By_Question", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
# finalDataByQuestion=CalDataByQuestion.join(DataByQuestion,on=["Assessment_type","Question_No","School_ID","Grade","Standards","Subject","Section_NID","Session"],how='inner')
# # Item Based Calculation()
# DataByItem=student_submissions_df.select("Assessment_type","Item_Name","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session","Points_Possible","Points_Received")
# DataByItemJoin=student_submissions_df.select("Assessment_type","Item_Name","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session")
# UserCalDataByItem=DataByItem.groupby("Assessment_type","Item_Name","User_UID","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Item_user_Possible_Point"),sum("Points_Received").alias("% Item_user_Correct_Answer"))
# UserfinalDataByItem=UserCalDataByItem.withColumn("% Item_user_Score_By_Item", round((col("% Item_user_Correct_Answer") / col("% Item_user_Possible_Point")) * 100, 2))
# UserfinalDataByItem=UserCalDataByItem.join(UserfinalDataByItem,on=["Assessment_type","User_UID","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session","% Item_user_Correct_Answer","% Item_user_Possible_Point"],how='inner')
# CalDataByItem=DataByItem.groupby("Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Item"),sum("Points_Received").alias("% Total_Correct_Answer_By_Item"))
# finalDataByItem=CalDataByItem.withColumn("% Total_Score_By_Item", round((col("% Total_Correct_Answer_By_Item") / col("% Total_Possible_Point_By_Item")) * 100, 2))
# finalDataByItem=CalDataByItem.join(finalDataByItem,on=["Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session","% Total_Possible_Point_By_Item","% Total_Correct_Answer_By_Item"],how='inner')
# finalDataByItem=UserfinalDataByItem.join(finalDataByItem,on=["Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session"],how='inner')
# # Standard Based Calculation()
# Score=student_submissions_df.groupby("Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Received").alias("Score"))
# Possible_Point=student_submissions_df.groupby("Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("Possible Points"))
# Score=Score.join(Possible_Point,on=["Standards","Assessment_type","School_ID","Grade","Subject","User_UID","Section_NID","Session"],how="left")
# Standards=Score.groupBy("User_UID","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session").agg(sum("Possible Points").alias("Standard_Possible Points"))
# Received=Score.groupBy("User_UID","School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Score").alias("Standard_Possible Receives"))
# Standards=Standards.join(Received,on=["User_UID","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session"])
# StandardsPercentage=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
# Standards=Standards.groupBy("School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Standard_Possible Points").alias("% Total_Possible_Point_By_Standard"),sum("Standard_Possible Receives").alias("% Total_Correct_Answer_By_Standard"))
# Standards=Standards.withColumn("% Total_Score_By_Standard", round((col("% Total_Correct_Answer_By_Standard") / col("% Total_Possible_Point_By_Standard")) * 100, 2))
# StandardsFinal=Standards.join(StandardsPercentage,on=["School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session"],how="inner")
# StandardsFinal=StandardsFinal.join(finalDataByQuestion,on=["Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session"],how="inner")
# StandardsFinal=StandardsFinal.join(finalDataByItem,on=["Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session"],how="inner")
# User_UID=student_submissions_df.groupBy("User_UID","School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").alias("User_Possible_Points"))
# User_UID_Received=student_submissions_df.groupBy("User_UID","School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Received").alias("User_Possible_Received"))
# User_UID=User_UID.join(User_UID_Received,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Section_NID","Session"])
# User_UID=User_UID.withColumn("% User_Correct Answer", round((col("User_Possible_Received") / col("User_Possible_Points")) * 100, 2))
# Total_Possible_Points=User_UID.groupBy("School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Points").alias("Total_Possible_Point"))
# Total_Possible_Received=User_UID.groupBy("School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Received").alias("Total_Correct_Answer"))
# TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
# TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
# TotalTable_JoinFinal=TotalTable_Join.join(User_UID,on=["School_ID","Grade","Subject","Assessment_type","Section_NID","Session"],how='inner')
# TotalTable_JoinFinal=TotalTable_JoinFinal.join(StandardsFinal,on=["School_ID","Grade","User_UID","Subject","Assessment_type","Section_NID","Session"],how='inner')
# submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
# student_df = student_df.select("name_display","User_UID")
# submissions_df=submissions_df.join(student_df,on=["User_UID"])
# submissions_df=submissions_df.join(item_df,on=["Item_Id","Item_Name","School_ID"])
# submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Section_NID","Subject","Assessment_type","Grade","Session","name_display").agg(countDistinct("Item_Name").alias("Test_Taken"))
# submissions_Test_Taken_dfFinal=submissions_Test_Taken_df.join(TotalTable_JoinFinal,on=["User_UID","School_ID","Section_NID","Subject","Assessment_type","Grade","Session"],how='inner')
# submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.join(section,on=["School_ID","Section_NID"],how='inner')
# # submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
# # submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
# High = 0.8
# Low = 0.7
# Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column

# # Define the conditions and assign color values accordingly
# result = (
# when(Selectedattribute/100 >= High, "#99FF99") \
# .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
# .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
# .otherwise(None)
# )
# submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.withColumn("Performance_Color", result)
# Selectedattribute = col("% Standard_Correct Answer")
# submissions_df=submissions_Test_Taken_dfFinal.withColumn("Standard_Performance_Color", result)
# Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Section_Instructors","Grade","Subject","Assessment_type").agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))
# submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Section_Instructors","Grade","Subject","Assessment_type"])
# Selectedattribute = col("%_Total_Correct_Answer")
# submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
# Selectedattribute = col("% Total_Score_By_Question")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
# Selectedattribute = col("% Total_Score_By_Item")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Item", result)
# Selectedattribute = col("% Total_Score_By_Standard")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
# Selectedattribute = col("% Teacher_Correct_Percentage")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)
# submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())
# schoology.publish(submissions_df,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report_longer',f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report_longer', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report_longer')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report_longer')

# agg_question_summary_report_longer_MutipleTeacher

# submissions_df = submissions_df.dropDuplicates(["Section_Instructors","Standards","School_ID","Grade","Subject","Assessment_type","Session"])
# schoology.publish(submissions_df,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report_longer_MutipleTeacher',f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report_longer_MutipleTeacher', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report_longer_MutipleTeacher')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report_longer_MutipleTeacher')


# In[296]:


spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_Standard_Deep_Dive")
spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_Standard_Deep_Dive")

# spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_question_summary_report_longer")
# spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_question_summary_report_longer")

# spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_incorrect_answer_details")
# spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_incorrect_answer_details")

# spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_incorrect_answer_details2")
# spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_incorrect_answer_details2")


# In[84]:


submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata=oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
standard= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
# agg for Total Student
total_students=submissions.join(item,on=["Item_ID","Item_Name","School_ID"],how="left")
total_students=total_students.join(section,on=["Section_NID","School_ID"])
total_students=total_students.groupby("Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Section_NID","Section_Instructors","Session").agg(countDistinct("User_UID").alias("Total_Student"))
total_students=total_students.na.drop(subset=["Assessment_type","School_ID","Grade","Subject","Section_NID","Session"])
total_students = total_students.withColumn("ID", monotonically_increasing_id())
schoology.publish(total_students,f'stage2/Enriched/schoology/v{schoology.version}/agg_Total_Student',f'stage3/Published/schoology/v{schoology.version}/agg_Total_Student', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Total_Student')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Total_Student')


# In[95]:


submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata=oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
standard= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard=standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")

item= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
questiondata=questiondata.join(standard,on=["Standards"],how="left")
submissions=submissions.join(questiondata, on=["Question_ID","Assessment_type","School_ID","Grade","Subject","Item_Id","Item_Name","Session"], how="inner")
submissions=submissions.join(item,on=["Item_ID","Item_Name","School_ID"])
submissions=submissions.na.drop(subset=["Assessment_type","Section_NID","Session","School_ID","Grade","Subject"])
standard1=submissions.select("Standards","School_ID","Grade","Subject","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime","subject")
#calculation by standard
calScorePossiblePoint=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Description","Strand","Session").agg(countDistinct("Question_NO").alias("Total_Question_By_Standard"),countDistinct("Standards").alias("Total_Sub_Standard"))

calScorePossiblePointBYSection=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Description","Strand","Section_NID","Session").agg(round(sum("Points_Possible"),2).alias("Possible_PointsByStandard"),round(sum("Points_Received"),2).alias("Score_ByStandard"),countDistinct("User_UID").alias("Total Student"))
TotalScorebyStandardBYSection=calScorePossiblePointBYSection.withColumn("% Total_Score_By_Standard", round((col("Score_ByStandard") / col("Possible_PointsByStandard")) * 100, 2))
TotalScorebyStandardBYSection=TotalScorebyStandardBYSection.withColumn("% Total_Incorrect_Score_By_Standard", round(100 -col("% Total_Score_By_Standard"), 2))
TotalScorebyStandardBYSection=TotalScorebyStandardBYSection.join(calScorePossiblePoint,on=["Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard",\
"Description","Strand","Session"],how="left")
TotalScorebyStandard=TotalScorebyStandardBYSection.fillna({'Standards': 'Other'})
Selectedattribute = col("% Total_Score_By_Standard")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
TotalScorebyStandard=TotalScorebyStandard.withColumn("Performance_Color_By_Standard", result)
calScorePossiblePointByStrand=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Strand","Session").agg(round(sum("Points_Possible"),2).alias("Possible_PointsByStrand"),round(sum("Points_Received"),2).alias("Score_ByStrand"),countDistinct("Question_NO").alias("Total_Question_By_Strand"),countDistinct("Standards").alias("Total_Sub_Strand"))
TotalScorebyStandardByStrand=calScorePossiblePointByStrand.withColumn("% Total_Score_By_Strand", round((col("Score_ByStrand") / col("Possible_PointsByStrand")) * 100, 2))
TotalScorebyStandardByStrand=TotalScorebyStandardByStrand.withColumn("% Total_Incorrect_Score_By_Strand", round(100-col("% Total_Score_By_Strand"), 2))
TotalScorebyStandardByStrand = TotalScorebyStandardByStrand.fillna({'Strand': 'Other'})
Selectedattribute = col("% Total_Score_By_Strand") 
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
TotalScorebyStandardByStrand=TotalScorebyStandardByStrand.withColumn("Performance_Color_By_Strand", result)

FinalResult=TotalScorebyStandard.join(TotalScorebyStandardByStrand,on=["Assessment_type","School_ID","Grade","Subject","Item_Name","Strand","Session"],how="left")
FinalResult=FinalResult.join(section,on=["Section_NID","School_ID"])
FinalResult = FinalResult.withColumn(
    "UniqueID",
    concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"), col("Section_NID"), col("cPalms_Standard"), col("Session"))
)
FinalResult = FinalResult.withColumn("ID", monotonically_increasing_id())
schoology.publish(FinalResult,f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive',f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive')


# In[70]:


# final incoorect naswer details published
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')  
dim_item = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
ques_data =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
dim_section = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
student_submissions_df =student_submissions_df.join(dim_section[["Section_Instructors","Section_NID"]],on="Section_NID")
all_student_submissions_df = student_submissions_df[['User_UID', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Session', 'Assessment_type', 'Subject', 'Grade', 'Section_NID',\
'Section_Instructors', 'Answer_Submission','Points_Received', 'Points_Possible']].dropDuplicates()

all_student_submissions_df = all_student_submissions_df.join(ques_data[["Question_ID","Question_No","Correct_Answer"]].dropDuplicates(),on=["Question_ID"])
total_possible_df = all_student_submissions_df.groupby(
    'Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade', 'Correct_Answer'
).agg(
    F.sum('Points_Possible').alias('Total_Possible_Points')
)
incorrect_submissions = all_student_submissions_df[all_student_submissions_df['Points_Received'] < all_student_submissions_df['Points_Possible']]
incorrect_submissions_df = incorrect_submissions[['User_UID',  'Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', \
'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission', 'Points_Received', 'Points_Possible']]

should_possible =  incorrect_submissions_df.groupby('Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', \
'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission').agg(
    F.sum('Points_Possible').alias('should_Possible_Points')
)
perc = should_possible.join(total_possible_df,on=['Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', \
'Section_Instructors', 'Grade', 'Correct_Answer'])
perc = perc.withColumn('Percentage_incorrect_options',
    round((col('should_Possible_Points') / col('Total_Possible_Points')) * 100, 2)
)
perc = perc.withColumn('Percentage_with_sign',
    concat(col('Percentage_incorrect_options').cast("string"), lit('%'))
)
perc = perc.drop("should_Possible_Points","Total_Possible_Points","Percentage_incorrect_options")
incorrect_students_df = incorrect_submissions_df.groupby(
    'Session', 'School_ID', 'Item_ID', 'Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission'
).agg(
    F.count('User_UID').alias('Number_of_students'),
)
students_percentage_both = perc.join(incorrect_students_df,on=['Session', 'School_ID', 'Item_ID', 'Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission']\
how="left")
incorrect_choices_df = incorrect_submissions_df.groupby(
     'Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade', 'Correct_Answer'
).agg(
    F.countDistinct('Answer_Submission').alias('Total_Incorrect_Choices'),
)
incorrect_table = students_percentage_both.join(incorrect_choices_df,on=['Session', 'School_ID', 'Item_ID', 'Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', 'Section_Instructors', 'Grade', 'Correct_Answer','Item_Name']\
how="left")
incorrect_table = incorrect_table.withColumn("ID", monotonically_increasing_id())

schoology.publish(incorrect_table,f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details',f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details')

# table - 2 (agg_Incorrect_Details - 2)
# Student Name and his incorrect answer submission
student_names = oea.load(f'stage2/Ingested/schoology/v0.1/student_submissions')  
student_names = student_names[["First_Name","Last_Name","User_UID"]].dropDuplicates()
student_names = student_names.withColumn('Student_Name', concat_ws(' ', col('First_Name'), col('Last_Name')))

incorrect_submissions = all_student_submissions_df[all_student_submissions_df['Points_Received'] < all_student_submissions_df['Points_Possible']]
incorrect_submissions_df = incorrect_submissions[['User_UID',  'Session', 'School_ID', 'Item_ID','Item_Name','Question_ID', 'Question_No', 'Assessment_type', 'Subject', 'Section_NID', \
'Section_Instructors', 'Grade', 'Correct_Answer', 'Answer_Submission', 'Points_Received', 'Points_Possible']]

incorrect_submissions_names = incorrect_submissions_df.join(student_names["Student_Name","User_UID"],on="User_UID",how="left")
incorrect_submissions_names = incorrect_submissions_names.drop("Points_Received", "Points_Possible")
incorrect_submissions_names = incorrect_submissions_names.withColumn("ID", monotonically_increasing_id())

schoology.publish(incorrect_submissions_names,f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details2',f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details2', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details2')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details2')


# In[268]:


spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_Question_Response_Analysis")
spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_Question_Response_Analysis")

# spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_Question_Response_Analysis_By_All")
# spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_Question_Response_Analysis_By_All")

spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_Standard_Deep_Dive")
spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_Standard_Deep_Dive")


# In[75]:


# agg_question_response_analysis_By_All
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard= oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard=standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df=question_df.join(standard,on=["Standards"],how="left")
student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","Correct_Answer","Grade","Subject","Standards","cPalms_Standard","Session",\
"Points_Possible","Points_Received")
CalDataByQuestion=student_submissions_df.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject",\
"Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion=CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
studentsubmission_Incorrect=student_submissions_df.filter(col("Points_Received")<col("Points_Possible"))
CalDataByQuestionByAnswer=studentsubmission_Incorrect.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Answer_Submission","Item_Name","Grade","Standards",\
"cPalms_Standard","Subject","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question_ByAnswer"),sum("Points_Received").\
alias("% Total_Correct_Answer_By_Question_ByAnswer"))
CalDataByQuestionByAnswer=CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name",\
"Grade","Standards","cPalms_Standard","Subject","Session"],how="left")
CalDataByQuestion_ByAnswer=CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("% Total_Possible_Point_By_Question_ByAnswer") / \
col("% Total_Possible_Point_By_Question")) * 100, 2))
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)

# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
results = Question_Data_Incorrect.withColumn("Result", concat(
    format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session") \
                        .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
CalDataByQuestion=CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject",\
"Session"],\
how="left")
student_submissions_df = submissions_df.withColumn(
    'assessment_attempt',
    date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject", "Item_Name","assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Session").agg(concat_ws(",", collect_list("assessment_attempt")).\
alias("assessment_date"))
CalDataByQuestion=CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Session"],how='inner')
Total_Possible_Points=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
CalDataByQuestion=CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"])
finalDataByQuestion = CalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
finalDataByQuestion = finalDataByQuestion.withColumn("ID", monotonically_increasing_id())
# schoology.publish(finalDataByQuestion,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All',f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All')


# In[ ]:


# Sapan's agg_question_response_analysis_By_All
# But I will make this now
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard = oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard=standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df=question_df.join(standard,on=["Standards"],how="left")
student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","Correct_Answer","Grade","Subject","Standards","cPalms_Standard","Session","Points_Possible","Points_Received")
CalDataByQuestion=student_submissions_df.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion=CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
studentsubmission_Incorrect=student_submissions_df.filter(col("Points_Received")<col("Points_Possible"))
CalDataByQuestionByAnswer=studentsubmission_Incorrect.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Answer_Submission","Item_Name","Grade","Standards",\
"cPalms_Standard","Subject","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question_ByAnswer"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question_ByAnswer"))
CalDataByQuestionByAnswer=CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name",\
"Grade","Standards","cPalms_Standard","Subject","Session"],how="left")
CalDataByQuestion_ByAnswer=CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("% Total_Possible_Point_By_Question_ByAnswer") / col("% Total_Possible_Point_By_Question")) * 100, 2))
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)

# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
results = Question_Data_Incorrect.withColumn("Result", concat(
    format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session") \
                        .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
CalDataByQuestion=CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session"],\
how="left")
student_submissions_df = submissions_df.withColumn(
    'assessment_attempt',
    date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject", "Item_Name","assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Session").agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
CalDataByQuestion=CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Session"],how='inner')
Total_Possible_Points=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
CalDataByQuestion=CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"])
finalDataByQuestion = CalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
finalDataByQuestion = finalDataByQuestion.withColumn("ID", monotonically_increasing_id())
# schoology.publish(finalDataByQuestion,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All',f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All')


# In[259]:


# Sapan's
# agg_question_response_analysis
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard= oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard=standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df=question_df.join(standard,on=["Standards"],how="left")
student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name",
"Session"])
student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","Correct_Answer","Grade","Subject","Standards",
"Description","cPalms_Standard","Section_NID","Session","Points_Possible","Points_Received")
CalDataByQuestion=student_submissions_df.groupby("Assessment_type","Question_ID","Question_No","Question","Correct_Answer","School_ID","Item_Name","Grade",
"Standards","Description","cPalms_Standard","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),
sum("Points_Received").alias("% Total_Correct_Answer_By_Question"),countDistinct("User_UID").alias("Total_Student"),countDistinct("Question_No").alias("Total_Question"))
CalDataByQuestion=CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
studentsubmission_Incorrect=student_submissions_df.filter(col("Points_Received") <col("Points_Possible"))
CalDataByQuestionByAnswer=studentsubmission_Incorrect.groupby("Assessment_type","Question_ID","Question_No","Question","Correct_Answer","School_ID",
"Answer_Submission","Item_Name","Grade","Standards","cPalms_Standard","Description","Subject","Section_NID","Session").agg(sum("Points_Possible").
alias("% Total_Possible_Point_By_Question_ByAnswer"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question_ByAnswer"))
CalDataByQuestionByAnswer=CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_ID","Question_No","Question","Correct_Answer",
"School_ID","Item_Name","Grade",\
"Standards","cPalms_Standard","Description","Subject","Section_NID","Session"],how="left")
CalDataByQuestion_ByAnswer=CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("% Total_Possible_Point_By_Question_ByAnswer") /
 col("% Total_Possible_Point_By_Question")) * 100, 2))
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
    .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
    .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
    .otherwise(None)
)
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)

# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
results = Question_Data_Incorrect.withColumn("Result", concat(
format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_ID","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Description","Subject","Section_NID","Session") \
                .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))

CalDataByQuestion=CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_ID","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards",\
"cPalms_Standard","Description","Subject","Section_NID","Session"],how="left")
student_submissions_df = submissions_df.withColumn(
'assessment_attempt',
date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject","Section_NID", "Section_NID", "Item_Name",
"assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Section_NID","Session").
agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
CalDataByQuestion=CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Section_NID","Session"],how='inner')
Total_Possible_Points=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").
agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").
agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
CalDataByQuestion=CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"])
finalDataByQuestion=CalDataByQuestion.join(section,on=["Section_NID","School_ID"])
finalDataByQuestion = finalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
finalDataByQuestion = finalDataByQuestion.withColumn(
"UniqueID",
concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Standards"), col("Session")))
finalDataByQuestion = finalDataByQuestion.withColumn("ID", monotonically_increasing_id())
schoology.publish(finalDataByQuestion,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis',\
f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis')


# In[75]:


#agg Table For agg_question_summary_report for Year To Date Report

section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
student_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_student')
student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
submissions_df=submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
student_df=student_df.withColumnRenamed('uid', 'User_UID')
student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
# Question Based Calculation()
DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session","Points_Possible","Points_Received")
CalDataByQuestion=DataByQuestion.groupby("Assessment_type","Question_No","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion=CalDataByQuestion.withColumn("% Total_Score_By_Question", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
finalDataByQuestion=CalDataByQuestion.join(DataByQuestion,on=["Assessment_type","Question_No","School_ID","Grade","Standards","Subject","Section_NID","Session"],how='inner')

# Item Based Calculation()
DataByItem=student_submissions_df.select("Assessment_type","Item_Name","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session","Points_Possible","Points_Received")
DataByItemJoin=student_submissions_df.select("Assessment_type","Item_Name","School_ID","User_UID","Grade","Subject","Standards","Section_NID","Session")
UserCalDataByItem=DataByItem.groupby("Assessment_type","Item_Name","User_UID","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Item_user_Possible_Point"),sum("Points_Received").alias("% Item_user_Correct_Answer"))
UserfinalDataByItem=UserCalDataByItem.withColumn("% Item_user_Score_By_Item", round((col("% Item_user_Correct_Answer") / col("% Item_user_Possible_Point")) * 100, 2))
UserfinalDataByItem=UserCalDataByItem.join(UserfinalDataByItem,on=["Assessment_type","User_UID","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session","% Item_user_Correct_Answer","% Item_user_Possible_Point"],how='inner')
CalDataByItem=DataByItem.groupby("Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Item"),sum("Points_Received").alias("% Total_Correct_Answer_By_Item"))
finalDataByItem=CalDataByItem.withColumn("% Total_Score_By_Item", round((col("% Total_Correct_Answer_By_Item") / col("% Total_Possible_Point_By_Item")) * 100, 2))
finalDataByItem=CalDataByItem.join(finalDataByItem,on=["Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session","% Total_Possible_Point_By_Item","% Total_Correct_Answer_By_Item"],how='inner')
finalDataByItem=UserfinalDataByItem.join(finalDataByItem,on=["Assessment_type","Item_Name","School_ID","Grade","Standards","Subject","Section_NID","Session"],how='inner')
# Standard Based Calculation()
Score=student_submissions_df.groupby("Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Received").alias("Score"))
Possible_Point=student_submissions_df.groupby("Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("Possible Points"))
Score=Score.join(Possible_Point,on=["Standards","Assessment_type","School_ID","Grade","Subject","User_UID","Section_NID","Session"],how="left")
Standards=Score.groupBy("User_UID","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session").agg(sum("Possible Points").alias("Standard_Possible Points"))
Received=Score.groupBy("User_UID","School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Score").alias("Standard_Possible Receives"))
Standards=Standards.join(Received,on=["User_UID","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session"])
StandardsPercentage=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
Standards=Standards.groupBy("School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Standard_Possible Points").alias("% Total_Possible_Point_By_Standard"),sum("Standard_Possible Receives").alias("% Total_Correct_Answer_By_Standard"))
Standards=Standards.withColumn("% Total_Score_By_Standard", round((col("% Total_Correct_Answer_By_Standard") / col("% Total_Possible_Point_By_Standard")) * 100, 2))
StandardsFinal=Standards.join(StandardsPercentage,on=["School_ID","Subject","Assessment_type","Grade","Standards","Section_NID","Session"],how="inner")

StandardsFinal=StandardsFinal.join(finalDataByQuestion,on=["Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session"],how="inner")
StandardsFinal=StandardsFinal.join(finalDataByItem,on=["Assessment_type","Standards","School_ID","User_UID","Grade","Subject","Section_NID","Session"],how="inner")

User_UID=student_submissions_df.groupBy("User_UID","School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").alias("User_Possible_Points"))


User_UID_Received=student_submissions_df.groupBy("User_UID","School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Received").alias("User_Possible_Received"))
User_UID=User_UID.join(User_UID_Received,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Section_NID","Session"])
User_UID=User_UID.withColumn("% User_Correct Answer", round((col("User_Possible_Received") / col("User_Possible_Points")) * 100, 2))

Total_Possible_Points=User_UID.groupBy("School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Points").alias("Total_Possible_Point"))
Total_Possible_Received=User_UID.groupBy("School_ID","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
TotalTable_JoinFinal=TotalTable_Join.join(User_UID,on=["School_ID","Grade","Subject","Assessment_type","Section_NID","Session"],how='inner')
TotalTable_JoinFinal=TotalTable_JoinFinal.join(StandardsFinal,on=["School_ID","Grade","User_UID","Subject","Assessment_type","Section_NID","Session"],how='inner')

submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
student_df = student_df.select("name_display","User_UID","School_ID")
submissions_df=submissions_df.join(student_df,on=["User_UID","School_ID"])
submissions_df=submissions_df.join(item_df,on=["Item_Id","Item_Name","School_ID"])
submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Section_NID","Subject","Assessment_type","Grade","Session","name_display").agg(countDistinct("Item_Name").alias("Test_Taken"))
submissions_Test_Taken_dfFinal=submissions_Test_Taken_df.join(TotalTable_JoinFinal,on=["User_UID","School_ID","Section_NID","Subject","Assessment_type","Grade","Session"],how='inner')
submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.join(section,on=["School_ID","Section_NID"],how='inner')
# submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
# submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
High = 0.8
Low = 0.7
Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column

# Define the conditions and assign color values accordingly
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.withColumn("Performance_Color", result)
Selectedattribute = col("% Standard_Correct Answer")
submissions_df=submissions_Test_Taken_dfFinal.withColumn("Standard_Performance_Color", result)
Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Section_Instructors","Grade","Subject","Assessment_type").agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))

submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Section_Instructors","Grade","Subject","Assessment_type"])

Selectedattribute = col("%_Total_Correct_Answer")
submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
Selectedattribute = col("% Total_Score_By_Question")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
Selectedattribute = col("% Total_Score_By_Item")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Item", result)
Selectedattribute = col("% Total_Score_By_Standard")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
Selectedattribute = col("% Teacher_Correct_Percentage")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)
submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())
schoology.publish(submissions_df,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report_longer',f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report_longer', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report_longer')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report_longer')

# agg_question_summary_report_longer_MutipleTeacher

submissions_df = submissions_df.dropDuplicates(["Section_Instructors","Standards","School_ID","Grade","Subject","Assessment_type","Session"])
schoology.publish(submissions_df,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report_longer_MutipleTeacher',f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report_longer_MutipleTeacher', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report_longer_MutipleTeacher')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report_longer_MutipleTeacher')


# In[260]:


submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata=oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data') 
# print("Questiondata is",questiondata.columns)

standard= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard=standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
# print("standard is",standard.columns)
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
questiondata = questiondata.join(standard,on=["Standards"],how="left")
# print("after joing with standards Questiondata is",questiondata.columns)
submissions = submissions.join(questiondata, on=["Question_ID","Assessment_type","School_ID","Grade","Subject","Item_Id","Item_Name","Session"], how="inner")
submissions = submissions.join(item,on=["Item_ID","Item_Name","School_ID"])
submissions = submissions.na.drop(subset=["Assessment_type","Section_NID","Session","School_ID","Grade","Subject"])
standard1 =submissions.select("Standards","School_ID","Grade","Subject","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime","subject")


#calculation by standard
calScorePossiblePoint=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Description","Strand","Session").\
agg(countDistinct("Question_NO").alias("Total_Question_By_Standard"),countDistinct("Standards").alias("Total_Sub_Standard"))


calScorePossiblePointBYSection=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Description","Strand","Section_NID","Session")\
.agg(round(sum("Points_Possible"),2).alias("Possible_PointsByStandard"),round(sum("Points_Received"),2).alias("Score_ByStandard"),countDistinct("User_UID").alias("Total Student"))
TotalScorebyStandardBYSection=calScorePossiblePointBYSection.withColumn("% Total_Score_By_Standard", round((col("Score_ByStandard") / col("Possible_PointsByStandard")) * 100, 2))
TotalScorebyStandardBYSection=TotalScorebyStandardBYSection.withColumn("% Total_Incorrect_Score_By_Standard", round(100 -col("% Total_Score_By_Standard"), 2))
TotalScorebyStandardBYSection=TotalScorebyStandardBYSection.join(calScorePossiblePoint,on=["Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard",\
"Description","Strand","Session"])
TotalScorebyStandard=TotalScorebyStandardBYSection.fillna({'Standards': 'Other'})
Selectedattribute = col("% Total_Score_By_Standard")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
TotalScorebyStandard=TotalScorebyStandard.withColumn("Performance_Color_By_Standard", result)
calScorePossiblePointByStrand=submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Strand","Session").agg(round(sum("Points_Possible"),2).\
alias("Possible_PointsByStrand"),round(sum("Points_Received"),2).alias("Score_ByStrand"),countDistinct("Question_NO").alias("Total_Question_By_Strand"),countDistinct("Standards").\
alias("Total_Sub_Strand"))
TotalScorebyStandardByStrand=calScorePossiblePointByStrand.withColumn("% Total_Score_By_Strand", round((col("Score_ByStrand") / col("Possible_PointsByStrand")) * 100, 2))
TotalScorebyStandardByStrand=TotalScorebyStandardByStrand.withColumn("% Total_Incorrect_Score_By_Strand", round(100-col("% Total_Score_By_Strand"), 2))
TotalScorebyStandardByStrand = TotalScorebyStandardByStrand.fillna({'Strand': 'Other'})
Selectedattribute = col("% Total_Score_By_Strand") 
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
TotalScorebyStandardByStrand=TotalScorebyStandardByStrand.withColumn("Performance_Color_By_Strand", result)

FinalResult=TotalScorebyStandard.join(TotalScorebyStandardByStrand,on=["Assessment_type","School_ID","Grade","Subject","Item_Name","Strand","Session"])
FinalResult=FinalResult.join(section,on=["Section_NID","School_ID"])
FinalResult = FinalResult.withColumn(
    "UniqueID",
    concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"), col("Section_NID"), col("Standards"), col("Session"))
)
FinalResult = FinalResult.fillna({'cPalms_Standard': 'Other',
'Description': 'Other',
'Strand': 'Other'
})

FinalResult.filter(
    (col("Grade")=='Grade 3') &
    (col("Subject")=='Math') &
    (col("Item_Name")=='Chapter 1')
).show()


FinalResult = FinalResult.withColumn("ID", monotonically_increasing_id())
# print("count is: ",FinalResult.count())
schoology.publish(FinalResult,f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive',f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive')


# In[272]:


# Suruchi Standard deep dive for accurate results
# standard deep dive -row 1 
# Strand wise calculation()
submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data') 
standard= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
# standard = standard.select("Standards","cPalms_Standard","Strand")
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')

distinct_questiondata = questiondata.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID', 'Standards'])
distinct_questiondata = distinct_questiondata.select('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID', 'Standards')
questiondata_standards = distinct_questiondata.join(
    standard[["Standards", "cPalms_Standard", "Strand"]],
    on=["Standards"],how="left"
)
submissions = submissions.dropDuplicates(['User_UID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Section_NID','Points_Received','Points_Possible'])
submissions = submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Submission_Grade','Section_NID','Points_Received','Points_Possible')
all_submissions = submissions.join(questiondata_standards,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID'])
#strand wise calculation()
strand_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'Strand').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers'),
    countDistinct(col('Question_ID')).alias('strandwise_wise_Total_Questions'),
    countDistinct(col('Standards')).alias('strandwise_Total_Standards'),
    countDistinct(col('User_UID')).alias('Total_Students')
)
# percentage sign ahead
strand_wise = strand_wise.withColumn('Percentage_Correct_Answers_by_strand',
    concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
) 
# strand_wise = strand_wise.drop("Percentage_Correct_Answers")
#teacherwise  Calculation()
filters_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('grade_avg'),
    countDistinct(col('Question_ID')).alias('Full_Total_Questions'),
    countDistinct(col('Standards')).alias('Full_Total_Standards'),
    countDistinct(col('User_UID')).alias('Full_Total_Students')
)
# percentage sign ahead
filters_wise = filters_wise.withColumn('grade_avg_perc',
    concat(col('grade_avg').cast("string"), lit('%'))
)
# filters_wise = filters_wise.drop("Percentage_Correct_Answers")
# joining strandwise and full table
final = filters_wise.join(strand_wise,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
final = final.fillna({'Strand': 'Other'})
final = final.withColumn("Performance_Color_By_strand", result)
final = final.drop("Percentage_Correct_Answers","grade_avg")
final.filter(col("Item_ID")=='6772003024').show(100)


# In[273]:


# standard deep dive -row 2 
#Standard wise calculation()
standard_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'Standards').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers'),
    countDistinct(col('Question_ID')).alias('standard_wise_wise_Total_Questions'),
    countDistinct(col('Standards')).alias('standard_wise_wise_Total_SubStandards'),
)
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
standard_wise = standard_wise.withColumn("Performance_Color_By_Standard", result)
# percentage sign ahead
standard_wise = standard_wise.withColumn('Percentage_Correct_Answers_by_Standard',
    concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
) 
standard_wise = standard_wise.drop("Percentage_Correct_Answers")
standard_wise.filter(col("Item_ID")=='6772003024').show(100)


# In[274]:


# standard deep dive -row 2 last_table
# Short_Standard wise calculation()
short_standard_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard').agg(
    sum(col('Points_Possible')).alias('Total_points_by_short_standrd'),
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers'),
    countDistinct(col('Question_ID')).alias('short_standard_wise_Total_Questions'),
)
# percentage sign ahead
short_standard_wise = short_standard_wise.withColumn('Percentage_Correct_Answers_by_short_Standard',
    concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
) 
short_standard_wise = short_standard_wise.drop("Percentage_Correct_Answers")
short_standard_wise = short_standard_wise.fillna({'cPalms_Standard': 'Other'})
# % of incorrect choices
incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]
Incorrect_Choices = incorrect_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard').agg(
    sum(col('Points_Possible')).alias('wrong_points_by_short_standard'))
short_standard_wise = short_standard_wise.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard'],how="left")
short_standard_wise_table = short_standard_wise.withColumn('Incorrect_Choices_Percentage_by_short_standard',
    concat(
        round((col('wrong_points_by_short_standard') / col('Total_points_by_short_standrd')) * 100, 2).cast("string"),
        lit('%')
    )
)
short_standard_wise_table.filter((col("Item_ID")=='6772003024') 
).show(100)


# In[275]:


# join three tables
# standard deep dive -row 2 
#Standard wise calculation()
joined = final.join(standard_wise,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
FinalResult = joined.join(short_standard_wise_table,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
#getting section_Instructors
FinalResult = FinalResult.join(section[["Section_NID","Section_Instructors"]],on="Section_NID")
# FinalResult.filter(col("Item_ID")=='6772003024').show(100)
FinalResult = FinalResult.withColumn("UniqueID",concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Standards"), col("Session")))
FinalResult = FinalResult.withColumn("ID", monotonically_increasing_id())
schoology.publish(FinalResult,f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive',f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive')


# In[276]:


# Sapan's
# agg_question_response_analysis
# But i will do changes here for best results
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard= oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
# question_df = question_df.join(standard,on=["Standards"],how="left")
distinct_questiondata = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# distinct_questiondata = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Question_No','Question',\
# 'Position_Number','Correct_Answer','Standards'])
distinct_questiondata = distinct_questiondata.select('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Position_Number')
submissions = student_submissions_df.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID',\
'Position_Number','Answer_Submission','Points_Received','Points_Possible'])
submissions = submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade',\
'Section_NID','Question_ID','Position_Number','Answer_Submission','Points_Received','Points_Possible')
submissions_questions = submissions.join(distinct_questiondata,on=['Session', 'School_ID','Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])

# student_submissions_df = student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
all_submissions = submissions_questions.join(item_df,on=["item_ID","Item_Name","School_ID"])
perc_correct_ans = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number').agg(\
    sum(col('Points_Possible')).alias('Total_points'),
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers')
)
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
    .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
    .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
    .otherwise(None)
)
perc_correct_ans = perc_correct_ans.withColumn("Performance_Color_By_Question", result)
perc_correct_ans = perc_correct_ans.withColumn('Percentage_of_Correct_Answers',               # % age sign
    concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
)
perc_correct_ans = perc_correct_ans.drop("Percentage_Correct_Answers") 
# Incorrect Choice Details
incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]
# incorrect_submissions = incorrect_submissions.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# 'Points_Received','Points_Possible','Answer_Submission'])
# incorrect_submissions = incorrect_submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# 'Points_Received','Points_Possible','Answer_Submission')
Incorrect_Choices = incorrect_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number','Answer_Submission').agg(
    sum(col('Points_Possible')).alias('wrong_points'))
incorrect_option_perc = perc_correct_ans.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number'],how="left")
incorrect_option_perc = incorrect_option_perc.withColumn('Incorrect_Choices_Percentage',
    concat(
        round((col('wrong_points') / col('Total_points')) * 100, 2).cast("string"),
        lit('%')
    )
)
results = incorrect_option_perc.withColumn("Result", concat(col("Incorrect_Choices_Percentage"), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number','Percentage_of_Correct_Answers','Performance_Color_By_Question') \
                .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
grouped_result = grouped_result.orderBy('Position_Number')
student_submissions_attempt = student_submissions_df.withColumn(
'assessment_attempt',
date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_attempt[['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID',"assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID')\
.agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
final_attempt = grouped_result.join(concatenated_df,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID'],how='inner')
final_attempt_ins = final_attempt.join(section[["Section_Instructors","Section_NID","School_ID"]],on=["Section_NID","School_ID"])
final_attempt_ins = final_attempt_ins.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
# join with standarsd to get standard and description
distinct_questiondata_info = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number','Question_No','Question',"Standards"])
question_stand = distinct_questiondata_info.join(standard[["Standards","Description"]],on=["Standards"],how="left")
question_stand = question_stand.fillna({'Standards': 'Others', 'Description': 'Others'})
final_attempt_ins = final_attempt_ins.join(question_stand,on=['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])

final_attempt_ins = final_attempt_ins.withColumn(
"UniqueID",
concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Standards"), col("Session")))
final_attempt_ins = final_attempt_ins.withColumn("ID", monotonically_increasing_id())

# perc_correct_ans.filter((col("Item_ID")=='6772003024')).orderBy("Question_ID").show(100)
# cnt = final_attempt_ins.filter((col("Question_ID")=='1914739193') )
# cnt.show(500,truncate=False)
# row_count = cnt.count()
# print(f"Number of rows: {row_count}")

schoology.publish(final_attempt_ins,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis',\
f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis')


# In[ ]:





# In[177]:


#final qra by handle multiple things

section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard = oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df = question_df.join(standard,on=["Standards"])


student_submissions_df = student_submissions_df.join(item_df,on=["Item_ID","Item_Name","School_ID"])      #  item_type came
CalDataByQuestion = student_submissions_df.groupby("Assessment_type","Question_ID","School_ID","Item_ID","Item_Name","Grade","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("Total_Possible_Points"),sum("Points_Received").alias("Total_Points_Received"),countDistinct("User_UID").alias("Total_Student"),countDistinct("Question_ID").alias("Total_Question"))
CalDataByQuestion = CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("Total_Points_Received") / col("Total_Possible_Points")) * 100, 2))

studentsubmission_Incorrect = student_submissions_df.filter(col("Points_Received") < col("Points_Possible"))
CalDataByQuestionByAnswer = studentsubmission_Incorrect.groupby("Assessment_type","Question_ID","School_ID","Answer_Submission","Item_Name","Grade","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("Total_Possible_Point_In_Incorr"),sum("Points_Received").alias("Total_received_Point_In_Incorr"))
CalDataByQuestionByAnswer = CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_ID","School_ID","Item_Name","Grade","Subject","Section_NID","Session"],how="right")


CalDataByQuestion_ByAnswer = CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("Total_Possible_Point_In_Incorr") / col("Total_Possible_Points")) * 100, 2))
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
    .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
    .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
    .otherwise(None)
)
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)

# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
print(CalDataByQuestion_ByAnswer.columns)
results = Question_Data_Incorrect.withColumn("Result", concat(
format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_Id","School_ID","Item_Name","Grade","Subject","Section_NID","Session") \
                .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))

CalDataByQuestion = CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_ID","School_ID","Item_Name","Grade","Subject","Section_NID","Session"])
student_submissions_df = submissions_df.withColumn('assessment_attempt',date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject","Section_NID", "Section_NID", "Item_Name","assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Section_NID","Session").agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))

CalDataByQuestion = CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Section_NID","Session"],how='inner')
Total_Possible_Points = student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received = student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join = Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
TotalTable_Join = TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))

CalDataByQuestion = CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"])
finalDataByQuestion = CalDataByQuestion.join(section,on=["Section_NID","School_ID"])
finalDataByQuestion = finalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
finalDataByQuestion = finalDataByQuestion.withColumn(
"UniqueID",
concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Session")))

deduplicated_question_df = question_df[["Item_ID", "Item_Name", "Standards", "Session", "Assessment_type", "Subject", "Grade", "School_ID", "Question_ID", "Correct_Answer", "cPalms_Standard","Description","Question_No","Question"]].dropDuplicates()
finalDataByQuestion = finalDataByQuestion.join(deduplicated_question_df,on=["Question_ID","Item_ID","Item_Name",'Session', 'Assessment_type', 'Subject', 'Grade',"School_ID"])
finalDataByQuestion =finalDataByQuestion.withColumnRenamed("Total_Possible_Points","%_Total_Possible_Point_By_Question").withColumnRenamed("Total_Points_Received","%_Total_Correct_Answer_By_Question")
finalDataByQuestion = finalDataByQuestion.drop("Item_ID")
finalDataByQuestion = finalDataByQuestion.withColumn("ID", monotonically_increasing_id())
print(finalDataByQuestion.count())
# filtered_df = finalDataByQuestion.filter(
#     (col("Grade") == 'Grade 3') &
#     (col("Item_Name") == 'Chapter 1') &
#     (col("Subject") == 'Math') &
#     (col("Session") == '2023-24') &
#     (col("Question_No") == '2')
# )

# # Show the filtered DataFrame
# filtered_df.show(truncate=False)
# schoology.publish(finalDataByQuestion,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_response_analysis',f'stage3/Published/schoology/v{schoology.version}/agg_question_response_analysis', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_response_analysis')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_response_analysis')


# In[38]:


# Question Response Analysis by All
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df = question_df.join(standard,on=["Standards"])
deduplicated_question_df = question_df[["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"]].dropDuplicates()

print("deduplicated_question_df",deduplicated_question_df.columns)
student_submissions_df = student_submissions_df.join(deduplicated_question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
print(student_submissions_df.columns)

# drop duplicates in Question Rows
deduplicated_question_df = question_df[["Item_ID", "Item_Name", "Session", "Assessment_type", "Subject", "Grade", "School_ID", "Question_ID", "Correct_Answer"\
,"Question_No"]].dropDuplicates()
student_submissions_df = student_submissions_df.join(deduplicated_question_df,on=["Question_ID","Item_ID","Item_Name",'Session', 'Assessment_type', 'Subject', 'Grade',"School_ID"])
print(student_submissions_df.columns)

student_submissions_df = student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
DataByQuestion = student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","Correct_Answer","Grade","Subject","Standards","cPalms_Standard","Session",\
"Points_Possible","Points_Received")
CalDataByQuestion = student_submissions_df.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject",\
"Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion = CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
studentsubmission_Incorrect = student_submissions_df.filter(col("Points_Received") == 0)
CalDataByQuestionByAnswer = studentsubmission_Incorrect.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Answer_Submission","Item_Name",\
"Grade","Standards","cPalms_Standard","Subject","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question_ByAnswer"),sum("Points_Received").\
alias("% Total_Correct_Answer_By_Question_ByAnswer"))
CalDataByQuestionByAnswer = CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name",\
"Grade","Standards","cPalms_Standard","Subject","Session"])
CalDataByQuestion_ByAnswer = CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("% Total_Possible_Point_By_Question_ByAnswer") / \
col("% Total_Possible_Point_By_Question")) * 100, 2))
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)

# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
results = Question_Data_Incorrect.withColumn("Result", concat(
    format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session") \
                        .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
CalDataByQuestion=CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject",\
"Session"])
student_submissions_df = submissions_df.withColumn(
    'assessment_attempt',
    date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject", "Item_Name","assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Session").agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
CalDataByQuestion=CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Session"],how='inner')
Total_Possible_Points=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
CalDataByQuestion=CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"])
finalDataByQuestion = CalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))

filtered_df = finalDataByQuestion.filter(
            # (col("Assessment_type") == 'Unit') &
            (col("Session") == '2023-24') &
            (col("Grade") == 'Grade 3') &
            (col("Item_Name") == 'Chapter 1') &
            (col("Question_No")=='2')
        )
filtered_df.show(1)


# In[33]:


# Question Response Analysis by All
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
standard= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard=standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df = question_df.join(standard,on=["Standards"])
without_correct_answer_question_df = question_df.drop("Correct_Answer")
print("without_correct_answer_question_df",without_correct_answer_question_df.columns)

# student_submissions_df = student_submissions_df.join(without_correct_answer_question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
print(student_submissions_df.columns)

# drop duplicates in Question Rows to get correct Answer column
deduplicated_question_df = question_df[["Item_ID", "Item_Name", "Session", "Assessment_type", "Subject", "Grade", "School_ID", "Question_ID", "Correct_Answer"]].dropDuplicates()
student_submissions_df = student_submissions_df.join(deduplicated_question_df,on=["Question_ID","Item_ID","Item_Name",'Session', 'Assessment_type', 'Subject', 'Grade',"School_ID"])
print(student_submissions_df.columns)


student_submissions_df = student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
# submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
DataByQuestion = student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","Correct_Answer","Grade","Subject","Standards","cPalms_Standard","Session",\
"Points_Possible","Points_Received")
CalDataByQuestion = student_submissions_df.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject",\
"Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion = CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
studentsubmission_Incorrect = student_submissions_df.filter(col("Points_Received") == 0)
CalDataByQuestionByAnswer = studentsubmission_Incorrect.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Answer_Submission","Item_Name",\
"Grade","Standards","cPalms_Standard","Subject","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question_ByAnswer"),sum("Points_Received").\
alias("% Total_Correct_Answer_By_Question_ByAnswer"))
CalDataByQuestionByAnswer = CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name",\
"Grade","Standards","cPalms_Standard","Subject","Session"])
CalDataByQuestion_ByAnswer = CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("% Total_Possible_Point_By_Question_ByAnswer") / \
col("% Total_Possible_Point_By_Question")) * 100, 2))
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)

# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
results = Question_Data_Incorrect.withColumn("Result", concat(
    format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_No","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session") \
                        .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
CalDataByQuestion=CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_No","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session"])
student_submissions_df = submissions_df.withColumn(
    'assessment_attempt',
    date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject", "Item_Name","assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Session")\
.agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
CalDataByQuestion=CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Session"],how='inner')
Total_Possible_Points=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
CalDataByQuestion=CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"])
finalDataByQuestion = CalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))

filtered_df = finalDataByQuestion.filter(
            # (col("Assessment_type") == 'Unit') &
            (col("Session") == '2023-24') &
            (col("Grade") == 'Grade 3') &
            (col("Item_Name") == 'Chapter 1') &
            (col("Question_No")=='2')
        )
filtered_df.show(1)
# getting correct answer column
# deduplicated_question_df = question_df[["Item_ID", "Item_Name", "Standards", "Session", "Assessment_type", "Subject", "Grade", "School_ID", "Question_ID", "Correct_Answer", \
# "cPalms_Standard","Description","Question_No","Question"]].dropDuplicates()
# finalDataByQuestion = finalDataByQuestion.join(deduplicated_question_df,on=["Question_ID","Item_ID","Item_Name",'Session', 'Assessment_type', 'Subject', 'Grade',"School_ID"])

print(question_df.columns)


# In[132]:


# ques summary report by Suruchi
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')
 
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
student_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_student')
student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
# deduplicatedquestion_df = question_df[["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session","Question_No"]].dropDuplicates()
# student_submissions_df = student_submissions_df.join(deduplicatedquestion_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
deduplicated_question_df = question_df.groupBy(
    "Question_Id", "Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Item_Name", "Session", "Question_No"
).agg(
    first("Standards").alias("Standards")      # fetching one question of any standard
)
deduplicated_question_df.filter(
    (col("Session") == '2023-24') &
    (col("Grade") == 'Grade 6') &
    (col("Item_Name") == 'Unit 1: Early Humans and Societies') &
    (col('Item_ID')=='6772017399')).show(100,truncate=False)

student_submissions_df = student_submissions_df.join(
    deduplicated_question_df,
    on=["Question_Id", "Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Item_Name", "Session"]
)
# print("after joining with question_df",student_submissions_df.columns)
# print("for last quetsion by ine user")
# fil = student_submissions_df.filter(
#             (col("Session") == '2023-24') &
#             (col("Grade") == 'Grade 6') &
#             (col("Item_Name") == 'Unit 1: Early Humans and Societies') &
#             (col("User_UID")=='56939036') &
#             (col('Question_ID')=='1862180319')
#             )
# fil.show(200)
student_df =  student_df.withColumnRenamed('uid', 'User_UID')
student_submissions_df = student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
student_submissions_df = student_submissions_df.withColumn("Points_Received_by_one",col("Points_Received")/col("Points_Possible"))
student_submissions_df = student_submissions_df.drop("Points_Received")
student_submissions_df = student_submissions_df.withColumnRenamed("Points_Received_by_one","Points_Received")

# fil = student_submissions_df.filter(
#             (col("Session") == '2023-24') &
#             (col("Grade") == 'Grade 6') &
#             (col("Item_Name") == 'Unit 1: Early Humans and Societies') &
#             (col("Question_No")==40) &
#             (col("User_UID")=='56939036'))
# fil.show(200)
# Question Based Calculation()
DataByQuestion = student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Subject","Standards","Section_NID","Session",\
"Points_Possible","Points_Received")
CalDataByQuestion = DataByQuestion.groupby("User_UID","Assessment_type","Question_No","School_ID","Item_Name","Grade","Standards","Subject","Section_NID","Session").\
agg(sum("Points_Possible").\
alias("% Total_Possible_Point_By_Question")\
,sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion = CalDataByQuestion.withColumn("% Total_Score_By_Question", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
finalDataByQuestion = CalDataByQuestion.join(DataByQuestion,on=["Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Standards","Subject","Section_NID",\
"Session"],how='inner')
 
# Standard Based Calculation()
Score=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Received")\
.alias("Score"))
Possible_Point=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").\
agg(sum("Points_Possible").alias("Possible Points"))
Score=Score.join(Possible_Point,on=["Standards","Assessment_type","School_ID","Item_Name","Grade","Subject","User_UID","Section_NID","Session"],how="left")
Standards=Score.groupBy("User_UID","School_ID","Item_Name","Standards","Subject","Assessment_type","Grade","Section_NID","Session").agg(sum("Possible Points").
alias("Standard_Possible Points"))
Received=Score.groupBy("User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Score").\
alias("Standard_Possible Receives"))
Standards=Standards.join(Received,on=["User_UID","Item_Name","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session"])
StandardsPercentage=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
Standards=Standards.groupBy("School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session").agg(sum("Standard_Possible Points").alias("% Total_Possible_Point_By_Standard"),\
sum("Standard_Possible Receives").alias("% Total_Correct_Answer_By_Standard"))
Standards = Standards.withColumn("% Total_Score_By_Standard", round((col("% Total_Correct_Answer_By_Standard") / col("% Total_Possible_Point_By_Standard")) * 100, 2))
StandardsFinal = Standards.join(StandardsPercentage,on=["School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session"],how="inner")
 
StandardsFinal=StandardsFinal.join(finalDataByQuestion,on=["Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session"],how="inner")

# User_UID = student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").\
# alias("User_Possible_Points"))
User_UID = student_submissions_df.groupBy(
    "User_UID", "School_ID", "Item_Name", "Grade", "Subject", "Assessment_type", "Section_NID", "Session"
).agg(
    countDistinct("Question_ID").alias("User_Possible_Points")
)
# print("User_UID----------")
# User_UID.filter(
#     (col("Session") == '2023-24') &
#     (col("Grade") == 'Grade 6') &
#     (col("Item_Name") == 'Unit 1: Early Humans and Societies') &
#     (col("User_UID")=='56939036')  
# ).show()
 
User_UID_Received = student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").\
agg(sum("Points_Received").alias("User_Possible_Received"))

# User_UID_Received.filter(
#     (col("Session") == '2023-24') &
#     (col("Grade") == 'Grade 6') &
#     (col("Item_Name") == 'Unit 1: Early Humans and Societies') &
#     (col("User_UID")=='56939036')  
# ).show()

User_UID = User_UID.join(User_UID_Received,on=["User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Section_NID","Session"])
User_UID = User_UID.withColumn("% User_Correct Answer", round((col("User_Possible_Received") / col("User_Possible_Points")) * 100, 2))
Total_Possible_Points = User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Points").\
alias("Total_Possible_Point"))
print("Total Possible Point")
Total_Possible_Points.filter(
    (col("Session") == '2023-24') &
    (col("Grade") == 'Grade 6') &
    (col("Item_Name") == 'Unit 1: Early Humans and Societies') &
    (col('Section_NID')=='6772012837')
    # (col("User_UID")=='56939036')  
).show()

Total_Possible_Received=User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Received").\
alias("Total_Correct_Answer"))
print("TTotal_Possible_Received--------")
Total_Possible_Received.filter(
    (col("Session") == '2023-24') &
    (col("Grade") == 'Grade 6') &
    (col("Item_Name") == 'Unit 1: Early Humans and Societies') &
    (col('Section_NID')=='6772012837') 
).show()

TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
TotalTable_JoinFinal=TotalTable_Join.join(User_UID,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how='inner')
TotalTable_JoinFinal=TotalTable_JoinFinal.join(StandardsFinal,on=["School_ID","Grade","User_UID","Item_Name","Subject","Assessment_type","Section_NID","Session"],how='inner')
submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
student_df = student_df.select("name_display","User_UID","School_ID")
submissions_df=submissions_df.join(student_df,on=["User_UID","School_ID"])
submissions_df=submissions_df.join(item_df,on=["Item_Id","Item_Name","School_ID"])
submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Section_NID","Subject","Assessment_type","Item_Name","Grade","Session","name_display").\
agg(countDistinct("Item_Name").alias("Test_Taken"))
submissions_Test_Taken_dfFinal=submissions_Test_Taken_df.join(TotalTable_JoinFinal,on=["User_UID","School_ID","Item_Name","Section_NID","Subject","Assessment_type","Grade",\
"Session"],how='inner')
submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.join(section,on=["School_ID","Section_NID"],how='inner')
# submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
# submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
High = 0.8
Low = 0.7
Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column
 
# Define the conditions and assign color values accordingly
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.withColumn("Performance_Color", result)
Selectedattribute = col("% Standard_Correct Answer")
submissions_df=submissions_Test_Taken_dfFinal.withColumn("Standard_Performance_Color", result)
Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type").\
agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))
 
submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type"])
 
Selectedattribute = col("%_Total_Correct_Answer")
submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
Selectedattribute = col("% Total_Score_By_Question")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
Selectedattribute = col("% Total_Score_By_Standard")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
Selectedattribute = col("% Teacher_Correct_Percentage")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)
submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())

filtered_df = submissions_df.filter(
    (col("Session") == '2023-24') &
    (col("Grade") == 'Grade 6') &
    (col("Item_Name") == 'Unit 1: Early Humans and Societies') &
    (col("User_UID")=='56939036')  
)
selected_df = filtered_df.select(
    "Question_No","Points_Received", "Points_Possible","Total_Possible_Point","Total_Correct_Answer","Total_Score","User_Possible_Points","User_Possible_Received","Standards","% Total_Possible_Point_By_Standard",\
    "% Total_Correct_Answer_By_Standard","% Total_Score_By_Standard","Standard_Possible Points","Standard_Possible Receives","% Standard_Correct Answer","Question_No","% Total_Possible_Point_By_Question"\
    # ,"%_Total_Correct_Answer_By_Question","%_Total_Score_By_Question
)
sorted_df = selected_df.orderBy("Question_No")
sorted_df.show(200)


# In[71]:


# checking columns of df to be published with same schema
# ques summary report by Suruchi
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')
 
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
student_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_student')
student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
# deduplicatedquestion_df = question_df[["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session","Question_No"]].dropDuplicates()
# student_submissions_df = student_submissions_df.join(deduplicatedquestion_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
deduplicated_question_df = question_df.groupBy(
    "Question_Id", "Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Item_Name", "Session", "Question_No"
).agg(
    first("Standards").alias("Standards")      # fetching one question of any standard
)
student_submissions_df = student_submissions_df.join(
    deduplicated_question_df,
    on=["Question_Id", "Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Item_Name", "Session"]
)
student_df =  student_df.withColumnRenamed('uid', 'User_UID')
student_submissions_df = student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
student_submissions_df = student_submissions_df.withColumn("Points_Received_by_one",col("Points_Received")/col("Points_Possible"))
student_submissions_df = student_submissions_df.drop("Points_Received")
student_submissions_df = student_submissions_df.withColumnRenamed("Points_Received_by_one","Points_Received")
student_submissions_df = student_submissions_df.withColumnRenamed("Points_Received_by_one","Points_Received")

# Question Based Calculation()
DataByQuestion = student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Subject","Standards","Section_NID","Session",\
"Points_Possible","Points_Received")
CalDataByQuestion = DataByQuestion.groupby("User_UID","Assessment_type","Question_No","School_ID","Item_Name","Grade","Standards","Subject","Section_NID","Session").\
agg(sum("Points_Possible").\
alias("% Total_Possible_Point_By_Question")\
,sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion = CalDataByQuestion.withColumn("% Total_Score_By_Question", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
finalDataByQuestion = CalDataByQuestion.join(DataByQuestion,on=["Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Standards","Subject","Section_NID",\
"Session"],how='inner')
 
# Standard Based Calculation()
Score=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Received")\
.alias("Score"))
Possible_Point=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").\
agg(sum("Points_Possible").alias("Possible Points"))
Score=Score.join(Possible_Point,on=["Standards","Assessment_type","School_ID","Item_Name","Grade","Subject","User_UID","Section_NID","Session"],how="left")
Standards=Score.groupBy("User_UID","School_ID","Item_Name","Standards","Subject","Assessment_type","Grade","Section_NID","Session").agg(sum("Possible Points").
alias("Standard_Possible Points"))
Received=Score.groupBy("User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Score").\
alias("Standard_Possible Receives"))
Standards=Standards.join(Received,on=["User_UID","Item_Name","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session"])
StandardsPercentage=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
Standards=Standards.groupBy("School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session").agg(sum("Standard_Possible Points").alias("% Total_Possible_Point_By_Standard"),\
sum("Standard_Possible Receives").alias("% Total_Correct_Answer_By_Standard"))
Standards = Standards.withColumn("% Total_Score_By_Standard", round((col("% Total_Correct_Answer_By_Standard") / col("% Total_Possible_Point_By_Standard")) * 100, 2))
StandardsFinal = Standards.join(StandardsPercentage,on=["School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session"],how="inner")
 
StandardsFinal=StandardsFinal.join(finalDataByQuestion,on=["Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session"],how="inner")

# User_UID = student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").\
# alias("User_Possible_Points"))
User_UID = student_submissions_df.groupBy(
    "User_UID", "School_ID", "Item_Name", "Grade", "Subject", "Assessment_type", "Section_NID", "Session"
).agg(
    countDistinct("Question_ID").alias("User_Possible_Points")
)

 
User_UID_Received = student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").\
agg(sum("Points_Received").alias("User_Possible_Received"))
User_UID = User_UID.join(User_UID_Received,on=["User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Section_NID","Session"])
User_UID = User_UID.withColumn("% User_Correct Answer", round((col("User_Possible_Received") / col("User_Possible_Points")) * 100, 2))
Total_Possible_Points = User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Points").\
alias("Total_Possible_Point"))


Total_Possible_Received=User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Received").\
alias("Total_Correct_Answer"))

TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
TotalTable_JoinFinal=TotalTable_Join.join(User_UID,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how='inner')
TotalTable_JoinFinal=TotalTable_JoinFinal.join(StandardsFinal,on=["School_ID","Grade","User_UID","Item_Name","Subject","Assessment_type","Section_NID","Session"],how='inner')
submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
student_df = student_df.select("name_display","User_UID","School_ID")
submissions_df=submissions_df.join(student_df,on=["User_UID","School_ID"])
submissions_df=submissions_df.join(item_df,on=["Item_Id","Item_Name","School_ID"])
submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Section_NID","Subject","Assessment_type","Item_Name","Grade","Session","name_display").\
agg(countDistinct("Item_Name").alias("Test_Taken"))
submissions_Test_Taken_dfFinal=submissions_Test_Taken_df.join(TotalTable_JoinFinal,on=["User_UID","School_ID","Item_Name","Section_NID","Subject","Assessment_type","Grade",\
"Session"],how='inner')
submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.join(section,on=["School_ID","Section_NID"],how='inner')
# submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
# submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
High = 0.8
Low = 0.7
Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column
 
# Define the conditions and assign color values accordingly
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.withColumn("Performance_Color", result)
Selectedattribute = col("% Standard_Correct Answer")
submissions_df=submissions_Test_Taken_dfFinal.withColumn("Standard_Performance_Color", result)
Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type").\
agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))
 
submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type"])
 
Selectedattribute = col("%_Total_Correct_Answer")
submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
Selectedattribute = col("% Total_Score_By_Question")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
Selectedattribute = col("% Total_Score_By_Standard")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
Selectedattribute = col("% Teacher_Correct_Percentage")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)
submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())
submissions_df = submissions_df.withColumn("Points_Possible", lit("1"))

# print(submissions_df.columns)
# submissions_df.printSchema()
schoology.publish(submissions_df,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report',f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report')


# In[67]:


spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_question_summary_report")
spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_question_summary_report")


# In[12]:


section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')
 
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
student_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_student')
student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
student_df=student_df.withColumnRenamed('uid', 'User_UID')
student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
# Question Based Calculation()
DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Subject","Standards","Section_NID","Session","Points_Possible","Points_Received")
CalDataByQuestion=DataByQuestion.groupby("User_UID","Assessment_type","Question_No","School_ID","Item_Name","Grade","Standards","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion=CalDataByQuestion.withColumn("% Total_Score_By_Question", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
finalDataByQuestion=CalDataByQuestion.join(DataByQuestion,on=["Assessment_type","Question_No","School_ID","Item_Name","User_UID","Grade","Standards","Subject","Section_NID","Session"],how='inner')
 
# Standard Based Calculation()
Score=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Received").alias("Score"))
Possible_Point=student_submissions_df.groupby("Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("Possible Points"))
Score=Score.join(Possible_Point,on=["Standards","Assessment_type","School_ID","Item_Name","Grade","Subject","User_UID","Section_NID","Session"],how="left")
Standards=Score.groupBy("User_UID","School_ID","Item_Name","Standards","Subject","Assessment_type","Grade","Section_NID","Session").agg(sum("Possible Points").alias("Standard_Possible Points"))
Received=Score.groupBy("User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Standards","Section_NID","Session").agg(sum("Score").alias("Standard_Possible Receives"))
Standards=Standards.join(Received,on=["User_UID","Item_Name","School_ID","Standards","Subject","Assessment_type","Grade","Section_NID","Session"])
StandardsPercentage=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
Standards=Standards.groupBy("School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session").agg(sum("Standard_Possible Points").alias("% Total_Possible_Point_By_Standard"),sum("Standard_Possible Receives").alias("% Total_Correct_Answer_By_Standard"))
Standards=Standards.withColumn("% Total_Score_By_Standard", round((col("% Total_Correct_Answer_By_Standard") / col("% Total_Possible_Point_By_Standard")) * 100, 2))
StandardsFinal=Standards.join(StandardsPercentage,on=["School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session"],how="inner")
 
StandardsFinal=StandardsFinal.join(finalDataByQuestion,on=["Assessment_type","Standards","School_ID","Item_Name","User_UID","Grade","Subject","Section_NID","Session"],how="inner")
 
User_UID=student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").alias("User_Possible_Points"))
 
User_UID_Received=student_submissions_df.groupBy("User_UID","School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Received").alias("User_Possible_Received"))
User_UID=User_UID.join(User_UID_Received,on=["User_UID","School_ID","Item_Name","Subject","Assessment_type","Grade","Section_NID","Session"])
User_UID=User_UID.withColumn("% User_Correct Answer", round((col("User_Possible_Received") / col("User_Possible_Points")) * 100, 2))
 
Total_Possible_Points=User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Points").alias("Total_Possible_Point"))
Total_Possible_Received=User_UID.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("User_Possible_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
TotalTable_JoinFinal=TotalTable_Join.join(User_UID,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how='inner')
TotalTable_JoinFinal=TotalTable_JoinFinal.join(StandardsFinal,on=["School_ID","Grade","User_UID","Item_Name","Subject","Assessment_type","Section_NID","Session"],how='inner')
submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
student_df = student_df.select("name_display","User_UID","School_ID")
submissions_df=submissions_df.join(student_df,on=["User_UID","School_ID"])
submissions_df=submissions_df.join(item_df,on=["Item_Id","Item_Name","School_ID"])
submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Section_NID","Subject","Assessment_type","Item_Name","Grade","Session","name_display").agg(countDistinct("Item_Name").alias("Test_Taken"))
submissions_Test_Taken_dfFinal=submissions_Test_Taken_df.join(TotalTable_JoinFinal,on=["User_UID","School_ID","Item_Name","Section_NID","Subject","Assessment_type","Grade","Session"],how='inner')
submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.join(section,on=["School_ID","Section_NID"],how='inner')
# submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
# submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
High = 0.8
Low = 0.7
Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column
 
# Define the conditions and assign color values accordingly
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
submissions_Test_Taken_dfFinal=submissions_Test_Taken_dfFinal.withColumn("Performance_Color", result)
Selectedattribute = col("% Standard_Correct Answer")
submissions_df=submissions_Test_Taken_dfFinal.withColumn("Standard_Performance_Color", result)
Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type").agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))
 
submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Item_Name","Section_Instructors","Grade","Subject","Assessment_type"])
 
Selectedattribute = col("%_Total_Correct_Answer")
submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
Selectedattribute = col("% Total_Score_By_Question")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
Selectedattribute = col("% Total_Score_By_Standard")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
Selectedattribute = col("% Teacher_Correct_Percentage")
submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)
submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())

schoology.publish(submissions_df,f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report',f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_question_summary_report')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_question_summary_report')


# In[52]:


# schoology.load_users()
# schoology.load_roles()
# schoology.get_files_from_bulk("oea/Raw_Files")
# schoology.preprocess_schoology_dataset("stage1/Transactional/schoology_raw/v0.1")
schoology.ingest_schoology_dataset("schoology/v0.1")
# schoology.build_dimension_tables("stage2/Ingested")
# schoology.build_fact_tables("stage2/Ingested")


# In[ ]:





# In[85]:


# # agg_question_response_analysis -final final
# standards_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_standard')
# student_submissions_df = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
# question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
# dim_item = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# section_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
# student_submissions_df = student_submissions_df.join(section_df[["Section_Instructors","Section_NID"]],on="Section_NID")
# Score = student_submissions_df.groupby("Assessment_type","Session" ,"School_ID", "Grade", "Section_NID","Subject","Section_Instructors" ,"Item_ID", "Question_ID", "Answer_Submission").agg(F.countDistinct("User_UID").alias("opt_students_attempted"),
#     F.sum("Points_Received").alias("Score")
# )
# print("score")
# filtered_df = Score.filter(col("Question_ID") == '1862157938')
# filtered_df.show(1,truncate=False)
# total_points_table = student_submissions_df.groupby(
#     "Assessment_type", "Session", "School_ID", "Grade", "Subject","Section_NID", "Section_Instructors", "Item_ID", "Question_ID"
# ).agg(
#     F.countDistinct("User_UID").alias("Ques_total_Students"),
#     F.sum("Points_Received").alias("Total_Score"),
#     F.sum("Points_Possible").alias("Total_Possible_Points")
# )
# print("total_points_table")
# total_points_table.filter((col("Question_ID") == '1862157938')).show(1)

# joined_df = Score.join(
#     total_points_table,
#     ["Assessment_type","Session","School_ID", "Grade", "Subject", "Section_NID","Section_Instructors","Item_ID", "Question_ID"],
#     "inner")
# print("joined_df")
# joined_df.filter((col("Question_ID") == '1862157938')).show(1)

# correct_submissions = joined_df.filter(col("Score") != 0)
# percentage_correct_df = correct_submissions.groupBy("Assessment_type","Session","School_ID", "Grade", "Section_NID","Section_Instructors","Subject", "Item_ID", "Question_ID","Score","Answer_Submission","opt_students_attempted","Ques_total_Students","Total_Score","Total_Possible_Points").agg(
#         round((col("opt_students_attempted") / col("Ques_total_Students")) *100 ,1).alias("Percentage_Correct_Answers"))
# print("percentage_correct_df")
# # percentage_correct_df.filter((col("Question_ID") == '1862157938')).show(1)

# Selectedattribute = col("Percentage_Correct_Answers")  # Assuming you have already calculated the Score_Percentage column
# color_result = (
#         when(Selectedattribute/100 <.7,"#FFCCFF") \
#         .when(Selectedattribute/100 >.8,"LightGreen") \
#         .when(Selectedattribute/100 >= .7,"Yellow") \
#         .otherwise(None)
#         )
# percentage_correct_df = percentage_correct_df.withColumn("Performance_Color", color_result)
# incorrect_submissions = joined_df.filter(col("Score") == 0)
# incorrect_perc = incorrect_submissions.withColumn("Perc", col("opt_students_attempted") / col("Ques_total_Students") * 100)
# results = incorrect_perc.withColumn("Result", concat(
#     format_string("%.2f%%", col("Perc")), lit(" chose ["), col("Answer_Submission"), lit("]")))

# grouped_result = results.groupBy("Assessment_type","Session","School_ID", "Grade", "Subject","Section_NID","Section_Instructors", "Item_ID", "Question_ID") \
#                         .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
# final_result = grouped_result.join(
#     percentage_correct_df,
#     ["Assessment_type","Session","School_ID", "Grade", "Subject", "Section_NID","Section_Instructors","Item_ID", "Question_ID"],
#     "inner")
# # final_result.filter((col("Question_ID") == '1862157938')).show(1)

# grouped_question_df = question_df.groupBy("Session","Assessment_type","Subject","Grade","Section_NID",'Question_No","Question","Standards").agg(
#     concat_ws(", ", col("Standards")).alias("all_Standards")
# )
# grouped_question_df = grouped_question_df.drop("Standards")
# # Rename the "all_Standards" column to "Standards"
# grouped_question_df = grouped_question_df.withColumnRenamed("all_Standards", "Standards")
# ques_stand_df = final_result.join(grouped_question_df, on="Question_ID", "inner")
# # Drop one of the "Question_ID" columns
# ques_stand_df = ques_stand_df.drop(grouped_question_df["Question_ID"])
# ques_stand_desc = ques_stand_df.join(
#     standards_df.select("Schoology_Standard", "description","Strand"),
#     ques_stand_df["Standards"] == standards_df["Schoology_Standard"],
#     "left"
# )
# ques_stand_desc = ques_stand_desc.drop("Schoology_Standard")
# ques_stand_desc = ques_stand_desc.fillna({'Strand': 'Other'})
# ques_stand_desc = ques_stand_desc.fillna({'Standards': 'Other'})
# ques_stand_desc = ques_stand_desc.fillna({'description': 'Other'})
# ques_stand_desc = ques_stand_desc.join(dim_item[["Item_ID","Item_Name"]],on='Item_ID')
# ques_stand_desc = ques_stand_desc.withColumn("unique_id", concat(col("School_ID"), col("Grade"), col("Item_Name"),col("Session"), col("Subject"), col("Strand")))
# ques_stand_desc = ques_stand_desc.withColumn("Incorrect_Choice_details", expr("SUBSTRING(Incorrect_Choice_details, 1, 8000)"))
# ques_stand_desc= ques_stand_desc.fillna({'Strand': 'Other'})
# ques_stand_desc= ques_stand_desc.fillna({'Standards': 'Other'})
# ques_stand_desc = ques_stand_desc.drop("opt_students_attempted", "Ques_total_Students")

# # ques_stand_desc.filter((col("Question_ID") == '1862157938')).show()
# # Adding assesment_date
# student_submissions_df = student_submissions_df.withColumn(
#     'assessment_attempt',
#     date_format(col('Latest_Attempt'), 'dd/MM/yyyy')
# )
# student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID","Section_NID", "Grade", "Subject", "Section_Instructors","Item_ID","assessment_attempt"]].dropDuplicates()
# concatenated_df = student_submissions_date.groupby(
#     "Assessment_type", "Session", "School_ID", "Grade","Section_Instructors","Section_NID", "Subject", "Item_ID"
# ).agg(
#     concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date")
# )
# ques_stand_desc_attempt = ques_stand_desc.join(
#     concatenated_df,
#     on=["Assessment_type", "Session", "School_ID", "Grade", "Section_NID","Section_Instructors","Subject", "Item_ID"],
#     how="inner"
# )
# ques_stand_desc_attempt = ques_stand_desc_attempt.withColumn("ID", monotonically_increasing_id())
# # filtered_df = ques_stand_desc_attempt.filter(
#     # (col("Assessment_type") == 'Unit') &
#     # (col("Session") == '2023-24') &
#     # # (col("Grade") == 'Grade 3') &
#     # (col("Question_ID") == '1862157938')
# # )
# # filtered_df.show(1)
# schoology.publish(ques_stand_desc_attempt,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis',f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis')


# In[ ]:


# #question_Summary_Report - done!

# section =  oea.load(f'stage2/Enriched/schoology/v0.1/dim_section')
# question_df =  oea.load(f'stage2/Enriched/schoology/v0.1/dim_question_data')
# item_df =  oea.load(f'stage2/Enriched/schoology/v0.1/dim_item')
# submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
# student_df = oea.load(f'stage3/Published/schoology/v0.1/dim_student')
# student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
# submissions_df=submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Session"])
# student_df=student_df.withColumnRenamed('uid', 'User_UID')
# student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","School_ID","Grade","Subject","Item_ID","Session"])
# student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","School_ID"])
# submissions_df=submissions_df.join(item_df,on=["item_Id","School_ID"])
# Score=student_submissions_df.groupby("Assessment_type","Question_No","School_ID","User_UID","Grade","Subject","Item_Name","Section_NID","Standards","Session").agg(sum("Points_Received").alias("Score"))
# Possible_Point=student_submissions_df.groupby("Assessment_type","Question_No","School_ID","User_UID","Grade","Subject","Item_Name","Section_NID","Standards","Session").agg(sum("Points_Possible").alias("Possible Points"))
# Score=Score.join(Possible_Point,on=["Question_No","Assessment_type","School_ID","Grade","Subject","Item_Name","User_UID","Standards","Section_NID","Session"],how="left")
# Standards=Score.groupBy("User_UID","School_ID","Standards","Subject","Assessment_type","Item_Name","Grade","Section_NID","Session").agg(sum("Possible Points").alias("Standard_Possible Points"))
# Received=Score.groupBy("User_UID","School_ID","Subject","Assessment_type","Item_Name","Grade","Standards","Section_NID","Session").agg(sum("Score").alias("Standard_Possible Receives"))
# Standards=Standards.join(Received,on=["User_UID","School_ID","Standards","Subject","Assessment_type","Item_Name","Grade","Section_NID","Session"])
# Standards=Standards.withColumn("% Standard_Correct Answer", round((col("Standard_Possible Receives") / col("Standard_Possible Points")) * 100, 2))
# User_UID=Standards.groupBy("User_UID","School_ID","Grade","Subject","Assessment_type","Item_Name","Section_NID","Session").agg(sum("Standard_Possible Points").alias("User_Possible Points"))
# User_UID_Received=Standards.groupBy("User_UID","School_ID","Grade","Subject","Assessment_type","Item_Name","Section_NID","Session").agg(sum("Standard_Possible Receives").alias("User_Possible Received"))
# User_UID=User_UID.join(User_UID_Received,on=["User_UID","School_ID","Subject","Assessment_type","Item_Name","Grade","Section_NID","Session"])
# User_UID=User_UID.withColumn("% User_Correct Answer", round((col("User_Possible Received") / col("User_Possible Points")) * 100, 2))
# Standards=Standards.join(User_UID,on=["User_UID","School_ID","Subject","Assessment_type","Item_Name","Grade","Session"],how="inner")
# submissions_df=submissions_df.join(Standards,on=["User_UID","School_ID","Assessment_type","Grade","Item_Name","Subject","Standards","Session"])
# submissions_df=submissions_df.join(section,on=["Section_NID","School_ID"])
# student_df = student_df.select("name_display","User_UID","School_ID")
# submissions_df=submissions_df.join(student_df,on=["User_UID","School_ID"])
# submissions_df=submissions_df.join(item_df,on=["Item_Id","item_Name","School_ID"])
# submissions_Test_Taken_df=submissions_df.groupBy("User_UID","School_ID","Subject","Assessment_type","Grade","Session").agg(countDistinct("Item_Name").alias("Test_Taken"))
# submissions_df=submissions_df.join(submissions_Test_Taken_df,on=["User_UID","School_ID","Subject","Assessment_type","Grade","Session"])
# submissions_df=submissions_df.select("Session","Standard_Possible Points","Standard_Possible Receives","Test_Taken","Points_Received","Points_Possible","School_ID","Section_Instructors","name_display","Grade","User_UID","Subject","Standards","Assessment_type","Item_Id","Item_Name","User_Possible Points","User_Possible Received","% Standard_Correct Answer","% User_Correct Answer","Question_No")
# High = 0.8
# Low = 0.7
# Selectedattribute = col("% User_Correct Answer")  # Assuming you have already calculated the Score_Percentage column

# # Define the conditions and assign color values accordingly
# result = (
# when(Selectedattribute/100 >= High, "#00FF06") \
# .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
# .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#FFFF00") \
# .otherwise(None)
# )
# submissions_df=submissions_df.withColumn("Performance_Color", result)
# Selectedattribute = col("% Standard_Correct Answer")
# submissions_df=submissions_df.withColumn("Standard_Performance_Color", result)
# Teacher_Percentage=submissions_df.groupBy("Session","School_ID","Section_Instructors","Grade","Subject","Assessment_type","Item_Id","Item_Name").agg(round(avg("% User_Correct Answer"),2).alias("% Teacher_Correct_Percentage"))

# submissions_df=submissions_df.join(Teacher_Percentage,on=["Session","School_ID","Section_Instructors","Grade","Subject","Assessment_type","Item_Id","Item_Name"])

# Total_Possible_Point=submissions_df.groupBy("Session","School_ID","Grade","Subject","Assessment_type","Item_Id","Item_Name").agg(sum("User_Possible Points").alias("% Total_Possible_Point"),sum("User_Possible Received").alias("% Total_Correct_Answer"),round(avg("% User_Correct Answer"),2).alias("% Total_Score"))

# submissions_df=submissions_df.join(Total_Possible_Point,on=["Session","School_ID","Grade","Subject","Assessment_type","Item_Id","Item_Name"])

# Total_Possible_Point_BY_Question=submissions_df.groupBy("Session","School_ID","Question_No","Grade","Subject","Assessment_type","Item_Id","Item_Name").agg(sum("User_Possible Points").alias("% Total_Possible_Point_By_Question"),sum("User_Possible Received").alias("% Total_Correct_Answer_By_Question"),round(avg("% User_Correct Answer"),2).alias("% Total_Score_By_Question"))

# submissions_df=submissions_df.join(Total_Possible_Point_BY_Question,on=["Session","School_ID","Question_No","Grade","Subject","Assessment_type","Item_Id","Item_Name"])

# Total_Possible_Point_BY_Standard=submissions_df.groupBy("Session","School_ID","Standards","Grade","Subject","Assessment_type","Item_Id","Item_Name").agg(sum("User_Possible Points").alias("% Total_Possible_Point_By_Standard"),sum("User_Possible Received").alias("% Total_Correct_Answer_By_Standard"),round(avg("% User_Correct Answer"),2).alias("% Total_Score_By_Standard"))

# submissions_df=submissions_df.join(Total_Possible_Point_BY_Standard,on=["Session","School_ID","Standards","Grade","Subject","Assessment_type","Item_Id","Item_Name"])
# Selectedattribute = col("% Total_Score")
# submissions_df=submissions_df.withColumn("Total_Performance_Color", result)
# Selectedattribute = col("% Total_Score_By_Question")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Question", result)
# Selectedattribute = col("% Total_Score_By_Standard")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Standard", result)
# Selectedattribute = col("% Teacher_Correct_Percselfentage")
# submissions_df=submissions_df.withColumn("Total_Performance_Color_By_Teacher", result)

# submissions_df = submissions_df.withColumn("Section_Instructors", split("Section_Instructors", ","))
# submissions_df = submissions_df.withColumn("Section_Instructors", explode("Section_Instructors"))
# submissions_df = submissions_df.withColumn("ID", monotonically_increasing_id())
# # submissions_df.show(3,truncate=False)


# In[115]:


def drop_agg_table(table_name):
    spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.{table_name}")
    spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.{table_name}")
    oea.rm_if_exists(f'stage2/Enriched/schoology/{schoology.version}/{table_name}')
    oea.rm_if_exists(f'stage3/Published/schoology/{schoology.version}/{table_name}')


# In[13]:


# # agg_Incorrect_Details
# student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')  
# dim_item = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')

# all_student_submissions_df = student_submissions_df[['User_UID', 'School_ID', 'Item_ID','Question_ID', 'Session', 'Assessment_type', 'Subject', 'Grade', 'Section', 'Answer_Submission', 'Correct_Answer', 'Points_Received', 'Points_Possible']]
# total_possible_df = all_student_submissions_df.groupby(
#     'School_ID', 'Assessment_type','Item_ID', 'Question_ID', 'Session','Subject', 'Grade','Correct_Answer'
# ).agg(
#     F.sum('Points_Possible').alias('Total_Possible_Points')
# )
# incorrect_submissions = all_student_submissions_df[student_submissions_df['Points_Received'] != student_submissions_df['Points_Possible']]
# incorrect_submissions_df = incorrect_submissions[['User_UID', 'School_ID', 'Assessment_type','Item_ID', 'Question_ID', 'Session', 'Subject', 'Grade', 'Answer_Submission', 'Correct_Answer', 'Points_Received', 'Points_Possible']]

# incorrect_choices_df = incorrect_submissions_df.groupby(
#     'School_ID', 'Item_ID', 'Question_ID', 'Session', 'Assessment_type','Subject', 'Grade', 'Points_Possible',"Correct_Answer"
# ).agg(
#     F.countDistinct('Answer_Submission').alias('Total_Incorrect_Choices'),
# )

# incorrect_students_df = incorrect_submissions_df.groupby(
#     'School_ID', 'Item_ID', 'Question_ID', 'Session', 'Assessment_type','Subject', 'Grade', 'Answer_Submission','Points_Possible',"Correct_Answer"
# ).agg(
#     F.countDistinct('User_UID').alias('Total_Incorrect_Students'),
# )

# incorrect_students_df.show(2)
# # incorrect_students_df = incorrect_students_df.join(incorrect_choices_df,on=['School_ID', 'Item_ID', 'Question_ID', 'Session', 'Assessment_type','Subject', 'Grade','Points_Possible',"Correct_Answer"],how="inner")
# # incorrect_students_points_possible = incorrect_students_df.join(total_possible_df,on=['School_ID', 'Item_ID','Assessment_type', 'Question_ID', 'Session', 'Subject', 'Grade',"Correct_Answer"], how="inner")

# # incorrect_students_df_perc = incorrect_students_points_possible.withColumn('Perc_incorrect_answers', F.round((F.col('Total_Incorrect_Students') * F.col('Points_Possible')) / F.col('Total_Possible_Points') * 100, 2))
# # incorrect_students_df_perc = incorrect_students_df_perc.withColumn('Perc_incorrect_answers', F.concat(F.col('Perc_incorrect_answers'), F.lit('%')))
# # incorrect_students_df_perc_item = incorrect_students_df_perc.join(dim_item[["Item_ID","Item_Name"]],on='Item_ID')

# # # table - 2 
# # # Student Name and his incorrect answer submission
# # student_names = oea.load(f'stage3/Published/schoology/v0.1/agg_question_summary_report')  
# # student_names = student_names[["name_display","User_UID"]].dropDuplicates(["name_display","User_UID"])
# # incorrect_submissions_names = incorrect_submissions.join(student_names["name_display","User_UID"],on="User_UID",how="inner")
# # incorrect_submissions_names = incorrect_submissions_names.join(dim_item[["Item_ID","Item_Name"]],on='Item_ID')

# # incorrect_submissions_names = incorrect_submissions_names.withColumn("ID", monotonically_increasing_id())
# # incorrect_submissions_names = incorrect_submissions_names.drop("Points_Received", "Points_Possible")

# # incorrect_submissions_names.show()
# # schoology.publish(incorrect_submissions_names,f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details2',f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details2', primary_key='ID')
# # oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details2')
# # oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details2')


# In[14]:


# teacher_dict = {
#     "Evan Markowitz, Jannette Rivera": "Jannette Rivera",
#     "Maria Baclohan, Evan Markowitz": "Maria Baclohan",
#     "Evan Markowitz, Mason Reeder": "Mason Reeder",
#     "Melaina Fijalkowski, Kathleen Tsakonas": "Kathleen Tsakonas",
#     "Evan Markowitz, Elizabeth Sedlak": "Elizabeth Sedlak",
#     "Daniel Smith, Nicole Swidarski": "Nicole Swidarski",
#     "Susanna Birdwell, Evan Markowitz": "Susanna Birdwell",
#     "Evan Markowitz, Niki Paul": "Niki Paul",
#     "Evan Markowitz, Tiffany Schaefer": "Tiffany Schaefer",
#     "Evan Markowitz, Kathleen Tsakonas": "Kathleen Tsakonas",
#     "Susanna Birdwell, Melaina Fijalkowski": "Melaina Fijalkowski",
#     "Daniel Smith, Heather Watkins": "Heather Watkins",
#     "Evan Markowitz, Mary Sidhom": "Mary Sidhom",
#     "Carissa Farrell, Madison Wahn": "Madison Wahn",
#     "Mary Sidhom, Madison Wahn": "Madison Wahn",
#     "Evan Markowitz, Pattie Rossi": "Pattie Rossi",
#     "Gabriela Agostino, Sitara Qalander": "Gabriela Agostino",
#     "Sharon Long, Pattie Rossi": "Pattie Rossi",
#     "Ana Leiva, Mary Vaughn": "Mary Vaughn",
#     "Evan Markowitz, Sitara Qalander, Jannette Rivera": "Jannette Rivera",
#     "Maria Baclohan, Sharon Long": "Sharon Long",
#     "Gabriela Agostino, Evan Markowitz": "Gabriela Agostino",
#     "Ashley Lekhram, Evan Markowitz": "Ashley Lekhram",
#     "Ana Leiva, Evan Markowitz, Mary Vaughn": "Mary Vaughn",
#     "Evan Markowitz, Nicole Swidarski": "Nicole Swidarski",
#     "Carissa Farrell, Evan Markowitz": "Carissa Farrell",
#     "Elizabeth Bennet, Melaina Fijalkowski": "Elizabeth Bennet"
# }
# from pyspark.sql.functions import col, when,udf
# from pyspark.sql.types import StringType

# def replace_values(value):
#     for key, val in teacher_dict.items():
#         if value == key:
#             return val
#     return value

# # Define the UDF using the replace_values function
# replace_values_udf = udf(replace_values, StringType())

# # Apply the UDF to the DataFrame
# df = df.withColumn("Section_Instructors_Values", 
#                    when(col("Section_Instructors").contains(","),
#                         replace_values_udf(col("Section_Instructors")))
#                    .otherwise(col("Section_Instructors")))

# df[["Section_Instructors_Values"]].show(5, truncate=False)


# In[86]:


# standards_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_standard')
# student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')  
# question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
# dim_item = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# section_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
# student_submissions_df = student_submissions_df.join(section_df[["Section_Instructors","Section_NID"]],on="Section_NID")

# Score = student_submissions_df.groupby("Assessment_type","Session" ,"School_ID","Section_Instructors" ,"Section_NID", "Section_Instructors","Grade", "Subject", "Item_Name", "Question_No", "Answer_Submission").agg(round(sum("Points_Received"), 2).alias("Score"),
#     sum("Points_Possible").alias("Possible_Points"))

# total_points_table = student_submissions_df.groupby(
#     "Assessment_type","Session" ,"School_ID", "Grade", "Subject","Section_NID", "Section_Instructors", "Item_Name", "Question_No").agg(sum("Points_Possible").alias("Total_points"))
# joined_df = Score.join(
#     total_points_table,
#     ["Assessment_type","Session","School_ID", "Grade", "Subject","Section_NID", "Section_Instructors", "Item_Name", "Question_No"],
#     "inner")
# percentage_correct_df = joined_df.groupBy("Assessment_type","Session","School_ID", "Grade", "Section_NID", "Section_Instructors","Subject","Item_Name", "Question_No").agg(
#     sum("Score").alias("Total_Score"),
#     sum("Possible_Points").alias("Total_Possible_Points"))

# percentage_correct_df = percentage_correct_df.withColumn(
#     "Percentage_Correct_Answers",
#         round((col("Total_Score") / col("Total_Possible_Points")) *100 ,1)
# )
# Selectedattribute = col("Percentage_Correct_Answers")  # Assuming you have already calculated the Score_Percentage column
# color_result = (
#         when(Selectedattribute/100 <.7,"#FFCCFF") \
#         .when(Selectedattribute/100 >.8,"LightGreen") \
#         .when(Selectedattribute/100 >= .7,"Yellow") \
#         .otherwise(None)
#         )
# percentage_correct_df = percentage_correct_df.withColumn("Performance_Color", color_result)
# incorrect_submissions = joined_df.filter(col("Score") == 0)
# incorrect_perc = incorrect_submissions.withColumn("Perc", col("Possible_Points") / col("Total_points") * 100)
# results = incorrect_perc.withColumn("Result", concat(
#     format_string("%.2f%%", col("Perc")), lit(" chose ["), col("Answer_Submission"), lit("]")))

# grouped_result = results.groupBy("Assessment_type","Session","School_ID", "Section_NID", "Section_Instructors","Grade", "Subject", "Item_Name", "Question_No") \
#                         .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
# final_result = grouped_result.join(
#     percentage_correct_df,
#     ["Assessment_type","Session","School_ID", "Grade", "Subject","Item_Name", "Question_No"],
#     "inner")

# grouped_question_df = question_df.groupBy("Question_No","Question_ID","Correct_Answer", "Question","Standards").agg(
#     concat_ws(", ", col("Standards")).alias("all_Standards")
# )
# grouped_question_df = grouped_question_df.drop("Standards")
# # Rename the "all_Standards" column to "Standards"
# grouped_question_df = grouped_question_df.withColumnRenamed("all_Standards", "Standards")
# ques_stand_df = final_result.join(grouped_question_df, "Question_ID", "inner")
# # Drop one of the "Question_ID" columns
# ques_stand_df = ques_stand_df.drop(grouped_question_df["Question_ID"]) 
# ques_stand_desc = ques_stand_df.join(
#     standards_df.select("Schoology_Standard", "description","Strand"),
#     ques_stand_df["Standards"] == standards_df["Schoology_Standard"],
#     "left"
# )
# ques_stand_desc = ques_stand_desc.drop("Schoology_Standard")
# ques_stand_desc = ques_stand_desc.fillna({'Strand': 'Other'})
# ques_stand_desc = ques_stand_desc.fillna({'Standards': 'Other'})
# ques_stand_desc = ques_stand_desc.fillna({'description': 'Other'})
# ques_stand_desc = ques_stand_desc.join(dim_item[["Item_ID","Item_Name"]],on='Item_ID')
# ques_stand_desc = ques_stand_desc.withColumn("unique_id", concat(col("School_ID"), col("Grade"), col("Item_Name"),col("Session"), col("Subject"), col("Strand")))
# ques_stand_desc = ques_stand_desc.withColumn("Incorrect_Choice_details", expr("SUBSTRING(Incorrect_Choice_details, 1, 8000)"))
# ques_stand_desc= ques_stand_desc.fillna({'Strand': 'Other'})
# ques_stand_desc= ques_stand_desc.fillna({'Standards': 'Other'})
# ques_stand_desc = ques_stand_desc.drop("opt_students_attempted", "Ques_total_Students")


# # Adding assesment_date
# student_submissions_df = student_submissions_df.withColumn(
#     'assessment_attempt', 
#     date_format(col('Latest_Attempt'), 'dd/MM/yyyy')
# )
# student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject","Section_NID", "Section_Instructors", "Item_ID","assessment_attempt"]].dropDuplicates()
# concatenated_df = student_submissions_date.groupby(
#     "Assessment_type", "Session", "School_ID", "Grade","Section_NID", "Section_Instructors", "Subject", "Item_ID"
# ).agg(
#     concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date")
# )
# ques_stand_desc_attempt = ques_stand_desc.join(
#     concatenated_df,
#     on=["Assessment_type", "Session", "School_ID", "Grade","Section_NID", "Section_Instructors", "Subject", "Item_ID"],
#     how="inner"
# )
# ques_stand_desc_attempt = ques_stand_desc_attempt.withColumn("ID", monotonically_increasing_id())
# filtered_df = ques_stand_desc_attempt.filter(
#     (col("Assessment_type") == 'Unit') &
#     (col("Session") == '2023-24') &
#     # (col("Grade") == 'Grade 3') &
#     (col("Question_ID") == '1862157938')
# )
# filtered_df.show(1)
# # self.publish(ques_stand_desc_secID_inst,f'stage2/Enriched/schoology/v{self.version}/agg_ques_response_analysis',f'stage3/Published/schoology/v{self.version}/agg_ques_response_analysis', primary_key='unique_id')
# # oea.add_to_lake_db(f'stage2/Enriched/schoology/v{self.version}/agg_ques_response_analysis')
# # oea.add_to_lake_db(f'stage3/Published/schoology/v{self.version}/agg_ques_response_analysis')


# In[70]:


df_User = oea.load(f'stage2/Ingested/schoology/v{schoology.version}/users')
df_Question = oea.load(f'stage2/Ingested/schoology/v{schoology.version}/question_data')
df_Question = df_Question.filter(
    (F.col('Standards_Val').isNotNull()) | 
    ((F.col('Standards_Val').isNull()) & (F.col('Standards') == 'Standards17'))
)
df_Student_Submissions = oea.load(f'stage2/Ingested/schoology/v{schoology.version}/student_submissions')
df_Submission_Summary = oea.load(f'stage2/Ingested/schoology/v{schoology.version}/submission_summary')
df_Student_Info = oea.load(f'stage2/Ingested/schoology/v{schoology.version}/student_information')
df_Student_Submissions = df_Student_Submissions.withColumnRenamed('User_School_ID','School_ID').withColumnRenamed('User_School_Name','School_Name')
df_User = df_User.withColumnRenamed('school_id','School_ID')

df_Student_Submissions_schoolID = df_Student_Submissions.select("Question_ID", "School_ID").dropDuplicates()
df_Question_schoolID = df_Question.join(df_Student_Submissions_schoolID,df_Question["Question_ID"] == df_Student_Submissions_schoolID["Question_ID"],"inner")
df_Question_schoolID = df_Question_schoolID.select(
    [df_Question[col] for col in df_Question.columns] + [df_Student_Submissions_schoolID["School_ID"]])

# dim_question_data     
df_Question_schoolID = df_Question_schoolID.drop("Standards")
df_Question_schoolID = df_Question_schoolID.withColumnRenamed('Standards_Val','Standards')

df_dim_question_data = df_Question_schoolID[['Question',
'Position_Number',
'Item_ID',
'Item_Name',
'School_ID',
'Standards',
'Question_ID',
'Question_No',        
'Least_Points_Earned',
'Correct_Answer',
'Question_Type',
'Average_Points_Earned',
'Associated_Question_ID',
'Total_Points',
'Most_Points_Earned',
'Correctly_Answered',
'Sub-Question',
'Session',
'Assessment_type',
'Subject',
'Grade',
# 'Section_NID',
'Section'
]].drop_duplicates()
df_dim_question_data = df_dim_question_data.withColumn("Qkey", monotonically_increasing_id())
# df_dim_question_data = df_dim_question_data.withColumn('Qkey',concat(df_dim_question_data['Session'],df_dim_question_data['Assessment_type'],df_dim_question_data['Subject'],df_dim_question_data['Grade'],df_dim_question_data['Question_ID'],df_dim_question_data['Correct_Answer'],df_dim_question_data['ID']))
# df_dim_question_data.show()
schoology.publish(df_dim_question_data,f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data',f'stage3/Published/schoology/v{schoology.version}/dim_question_data', primary_key='Qkey')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')


# In[71]:





# In[82]:


# agg_question_response_analysis
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard= oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df = question_df.join(standard,on=["Standards"])
student_submissions_df = student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
student_submissions_df = student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","Correct_Answer","Grade","Subject","Standards","Description","cPalms_Standard","Section_NID","Session","Points_Possible","Points_Received")
CalDataByQuestion=student_submissions_df.groupby("Assessment_type","Question_No","Question","Correct_Answer","School_ID","Item_Name","Grade","Standards","Description","cPalms_Standard","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"),countDistinct("User_UID").alias("Total_Student"),countDistinct("Question_No").alias("Total_Question"))
CalDataByQuestion=CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
studentsubmission_Incorrect=student_submissions_df.filter(col("Points_Received") == 0)
CalDataByQuestionByAnswer=studentsubmission_Incorrect.groupby("Assessment_type","Question_No","Question","Correct_Answer","School_ID","Answer_Submission","Item_Name","Grade","Standards","cPalms_Standard","Description","Subject","Section_NID","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question_ByAnswer"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question_ByAnswer"))
CalDataByQuestionByAnswer=CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_No","Question","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Description","Subject","Section_NID","Session"])
CalDataByQuestion_ByAnswer=CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("% Total_Possible_Point_By_Question_ByAnswer") / col("% Total_Possible_Point_By_Question")) * 100, 2))

Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (when(Selectedattribute/100 >= High, "#99FF99") \
    .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
    .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
    .otherwise(None)
)
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)
 
# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
results = Question_Data_Incorrect.withColumn("Result", concat(format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Description","Subject","Section_NID","Session") \
                .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))

CalDataByQuestion=CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Description","Subject","Section_NID","Session"])
student_submissions_df = submissions_df.withColumn('assessment_attempt',date_format(col('Latest_Attempt'), 'dd/MM/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject","Section_NID", "Section_NID", "Item_Name","assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Section_NID","Session").agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
CalDataByQuestion=CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Section_NID","Session"],how='inner')
Total_Possible_Points=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session").agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
CalDataByQuestion=CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Section_NID","Session"])
finalDataByQuestion=CalDataByQuestion.join(section,on=["Section_NID","School_ID"])
finalDataByQuestion = finalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
finalDataByQuestion = finalDataByQuestion.withColumn("UniqueID",concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("cPalms_Standard"), col("Session")))
finalDataByQuestion = finalDataByQuestion.withColumn("ID", monotonically_increasing_id())

finalDataByQuestion.filter(
        (col('Question_No')=='7') &
        (col("Subject") == 'Math') &
        (col("Session") == '2023-24') &
        (col("Grade") == 'Grade 1')).show()
# schoology.publish(finalDataByQuestion,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis',f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis')


# In[ ]:


# agg_question_response_analysis_By_All
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard= oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard=standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df=question_df.join(standard,on=["Standards"],how='left')

student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","Correct_Answer","Grade","Subject","Standards","cPalms_Standard","Session","Points_Possible","Points_Received")
        
print(DataByQuestion.columns)
CalDataByQuestion=student_submissions_df.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion=CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
studentsubmission_Incorrect=student_submissions_df.filter(col("Points_Received") == 0)
CalDataByQuestionByAnswer=studentsubmission_Incorrect.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Answer_Submission","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question_ByAnswer"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question_ByAnswer"))

CalDataByQuestionByAnswer=CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session"])
CalDataByQuestion_ByAnswer=CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("% Total_Possible_Point_By_Question_ByAnswer") / col("% Total_Possible_Point_By_Question")) * 100, 2))

Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)
 
# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
results = Question_Data_Incorrect.withColumn("Result", concat(
    format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session") \
                        .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
CalDataByQuestion=CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session"])
CalDataByQuestion.filter(
        (col('Question_No')=='8') &
        (col("Subject") == 'Math') &
        (col("Session") == '2023-24') &
        (col("Grade") == 'Grade 1') &
        (col('Correct_Answer')=='c. 93 and 113')).show()

student_submissions_df = submissions_df.withColumn(
    'assessment_attempt',
    date_format(col('Latest_Attempt'), 'dd/MM/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject", "Item_Name","assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Session").agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
CalDataByQuestion=CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Session"],how='inner')
Total_Possible_Points=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
CalDataByQuestion=CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"])
finalDataByQuestion = CalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
finalDataByQuestion = finalDataByQuestion.withColumn("ID", monotonically_increasing_id())

finalDataByQuestion.filter(
        (col('Question_No')=='8') &
        (col("Subject") == 'Math') &
        (col("Session") == '2023-24') &
        (col("Grade") == 'Grade 1') &
        (col('Correct_Answer')=='c. 93 and 113')).show()

# schoology.publish(finalDataByQuestion,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All',f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All')


# In[30]:


# agg_Incorrect_Details
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')  
dim_item = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
ques_data =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
dim_section = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
student_submissions_df =student_submissions_df.join(dim_section[["Section_Instructors","Section_NID"]],on="Section_NID")
all_student_submissions_df = student_submissions_df[['User_UID', 'School_ID', 'Item_Name','Question_ID', 'Session', 'Assessment_type', 'Subject', 'Grade', 'Section_Instructors', 'Answer_Submission', 'Correct_Answer', 'Points_Received', 'Points_Possible']].dropDuplicates()

all_student_submissions_df = all_student_submissions_df.join(ques_data[["Question_ID","Question_No"]].dropDuplicates(),on=["Question_ID"])
total_possible_df = all_student_submissions_df.groupby(
    'Session','School_ID', 'Assessment_type','Item_Name', 'Question_No', 'Session','Subject', 'Grade','Section_Instructors','Correct_Answer'
).agg(
    F.sum('Points_Possible').alias('Total_Possible_Points')
)
incorrect_submissions = all_student_submissions_df[all_student_submissions_df['Points_Received'] != all_student_submissions_df['Points_Possible']]

incorrect_submissions_df = incorrect_submissions[['User_UID', 'School_ID', 'Assessment_type','Item_Name', 'Question_No', 'Session', 'Subject', 'Grade','Section_Instructors','Answer_Submission', 'Correct_Answer', 'Points_Received', 'Points_Possible']]
incorrect_choices_df = incorrect_submissions_df.groupby(
    'Session','School_ID', 'Item_Name', 'Question_No', 'Assessment_type','Subject', 'Section_Instructors','Grade',"Correct_Answer"
).agg(
    F.countDistinct('Answer_Submission').alias('Total_Incorrect_Choices'),
)

incorrect_students_df = incorrect_submissions_df.groupby(
    'School_ID','Item_Name', 'Question_No', 'Session', 'Assessment_type','Subject', 'Section_Instructors','Grade', 'Answer_Submission','Points_Possible',"Correct_Answer"
).agg(
    F.countDistinct('User_UID').alias('Total_Incorrect_Students'),
)
incorrect_students_points_possible = incorrect_students_df.join(total_possible_df,on=['School_ID', 'Item_Name','Assessment_type', 'Question_No', 'Session', 'Subject','Section_Instructors','Grade',"Correct_Answer"], how="inner")
incorrect_students_df_perc = incorrect_students_points_possible.withColumn('Perc_incorrect_answers', F.round((F.col('Total_Incorrect_Students') * F.col('Points_Possible')) / F.col('Total_Possible_Points') * 100, 2))
incorrect_students_df_perc = incorrect_students_df_perc.withColumn('Perc_incorrect_answers', F.concat(F.col('Perc_incorrect_answers'), F.lit('%')))
incorrect_students_df_perc = incorrect_students_df_perc.withColumn("ID", monotonically_increasing_id())
schoology.publish(incorrect_students_df_perc,f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details',f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details')
# table - 2 (agg_Incorrect_Details - 2)
# Student Name and his incorrect answer submission
student_names = oea.load(f'stage3/Published/schoology/v0.1/agg_question_summary_report')  
student_names = student_names[["name_display","User_UID"]].dropDuplicates(["name_display","User_UID"])
incorrect_submissions_names = incorrect_submissions_df.join(student_names["name_display","User_UID"],on="User_UID",how="inner")

incorrect_submissions_names = incorrect_submissions_names.withColumn("ID", monotonically_increasing_id())
incorrect_submissions_names = incorrect_submissions_names.drop("Points_Received", "Points_Possible")
schoology.publish(incorrect_submissions_names,f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details2',f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details2', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_incorrect_answer_details2')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_incorrect_answer_details2')


# In[29]:


spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_incorrect_answer_details")
spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_incorrect_answer_details")

spark.sql(f"drop table if exists ldb_dev_s3_schoology_v0p1.agg_incorrect_answer_details2")
spark.sql(f"drop table if exists ldb_dev_s2e_schoology_v0p1.agg_incorrect_answer_details2")


# In[86]:


q= "select * from ldb_dev_s3_schoology_v0p1.agg_question_response_analysis WHERE Item_Name IS NULL"
df = spark.sql(q)
df.show()


# In[87]:


q = "select * from ldb_dev_s3_schoology_v0p1.agg_question_response_analysis WHERE Incorrect_Choice_details IS NULL"
df = spark.sql(q)
df.show()


# In[10]:


# Suruchi Standard deep dive for accurate results
# standard deep dive - row 1 
# Strand wise calculation()student
submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data') 
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
# standard = standard.select("Standards","cPalms_Standard","Strand")
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
distinct_questiondata = questiondata.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID', 'Standards'])
distinct_questiondata = distinct_questiondata.select('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID', 'Standards')
questiondata_standards = distinct_questiondata.join(standard[["Standards", "cPalms_Standard", "Strand"]],on=["Standards"],how="left")
submissions = submissions.dropDuplicates(['User_UID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Section_NID','Points_Received','Points_Possible'])
submissions = submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Submission_Grade','Section_NID','Points_Received','Points_Possible')
all_submissions = submissions.join(questiondata_standards,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID'])
#strand wise calculation()
strand_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'Strand').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers_by_strand'),
    countDistinct(col('Question_ID')).alias('strandwise_wise_Total_Questions'),
    countDistinct(col('Standards')).alias('strandwise_Total_Standards'),
    countDistinct(col('User_UID')).alias('Total_Students')
)
# percentage sign ahead
# strand_wise = strand_wise.withColumn('Percentage_Correct_Answers_by_strand',
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# ) 
# strand_wise = strand_wise.drop("Percentage_Correct_Answers")
#teacherwise  Calculation()
filters_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('grade_avg'),
    countDistinct(col('Question_ID')).alias('Full_Total_Questions'),
    countDistinct(col('Standards')).alias('Full_Total_Standards'),
    countDistinct(col('User_UID')).alias('Full_Total_Students')
)
# percentage sign ahead
# filters_wise = filters_wise.withColumn('grade_avg_perc',
#     concat(col('grade_avg').cast("string"), lit('%'))
# )
# filters_wise = filters_wise.drop("Percentage_Correct_Answers")
# joining strandwise and full table
final = filters_wise.join(strand_wise,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
Selectedattribute = col("Percentage_Correct_Answers_by_strand")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
final = final.fillna({'Strand': 'Other'})
final = final.withColumn("Performance_Color_By_strand", result)
final = final.drop("grade_avg")
# standard deep dive -row 2 
#Standard wise calculation()
standard_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'Standards').agg(
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers_by_Standard'),
    countDistinct(col('Question_ID')).alias('standard_wise_wise_Total_Questions'),
    countDistinct(col('Standards')).alias('standard_wise_wise_Total_SubStandards'),
)
Selectedattribute = col("Percentage_Correct_Answers_by_Standard")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
standard_wise = standard_wise.withColumn("Performance_Color_By_Standard", result)
# percentage sign ahead
# standard_wise = standard_wise.withColumn('Percentage_Correct_Answers_by_Standard',
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# ) 
# standard_wise = standard_wise.drop("Percentage_Correct_Answers")
# standard deep dive -row 2 last_table
# Short_Standard wise calculation()
short_standard_wise = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard').agg(
    sum(col('Points_Possible')).alias('Total_points_by_short_standrd'),
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers_by_short_Standard'),
    countDistinct(col('Question_ID')).alias('short_standard_wise_Total_Questions'),
)
# percentage sign ahead
# short_standard_wise = short_standard_wise.withColumn('Percentage_Correct_Answers_by_short_Standard',
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# ) 
# short_standard_wise = short_standard_wise.drop("Percentage_Correct_Answers")
short_standard_wise = short_standard_wise.fillna({'cPalms_Standard': 'Other'})
# % of incorrect choices
incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]
Incorrect_Choices = incorrect_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard').agg(
    sum(col('Points_Possible')).alias('wrong_points_by_short_standard'))
short_standard_wise = short_standard_wise.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID', 'cPalms_Standard'],how="left")
short_standard_wise_table = short_standard_wise.withColumn('Incorrect_Choices_Percentage_by_short_standard',
        round((col('wrong_points_by_short_standard') / col('Total_points_by_short_standrd')) * 100, 2)
)
# join three tables
# standard deep dive -row 2 
#Standard wise calculation()
joined = final.join(standard_wise,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
FinalResult = joined.join(short_standard_wise_table,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID'])
#getting section_Instructors
FinalResult = FinalResult.join(section[["Section_NID","Section_Instructors"]],on="Section_NID")
# to get description column for hovering
FinalResult = FinalResult.join(standard[["Standards","description"]],on=["Standards"],how="left")
# FinalResult.filter(col("Item_ID")=='6772003024').show(100)
FinalResult = FinalResult.withColumn("UniqueID",concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Standards"), col("Session")))
FinalResult = FinalResult.withColumn("ID", monotonically_increasing_id())

FinalResult.filter(col('Item_ID')=='6075168580').show(2)


# In[29]:


# Suruchi'sagg_question_response_analysis
# But i will do changes here for best results
section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard= oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
# question_df = question_df.join(standard,on=["Standards"],how="left")
distinct_questiondata = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# distinct_questiondata = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Question_No','Question',\
# 'Position_Number','Correct_Answer','Standards'])
distinct_questiondata = distinct_questiondata.select('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Position_Number')
submissions = student_submissions_df.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID',\
'Position_Number','Answer_Submission','Points_Received','Points_Possible'])
submissions = submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade',\
'Section_NID','Question_ID','Position_Number','Answer_Submission','Points_Received','Points_Possible')
submissions_questions = submissions.join(distinct_questiondata,on=['Session', 'School_ID','Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# student_submissions_df = student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
all_submissions = submissions_questions.join(item_df,on=["item_ID","Item_Name","School_ID"])
perc_correct_ans = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number').agg(\
    sum(col('Points_Possible')).alias('Total_points'),
    round(sum(col('Points_Received')) / sum(col('Points_Possible')), 2).alias('Percentage_Correct_Answers')
)
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
    .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
    .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
    .otherwise(None)
)
perc_correct_ans = perc_correct_ans.withColumn("Performance_Color_By_Question", result)
# perc_correct_ans = perc_correct_ans.withColumn('Percentage_of_Correct_Answers',               # % age sign
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# )
# perc_correct_ans = perc_correct_ans.drop("Percentage_Correct_Answers") 
# Incorrect Choice Details
incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]
# incorrect_submissions = incorrect_submissions.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# 'Points_Received','Points_Possible','Answer_Submission'])
# incorrect_submissions = incorrect_submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# 'Points_Received','Points_Possible','Answer_Submission')
Incorrect_Choices = incorrect_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number',\
'Answer_Submission').agg(
    sum(col('Points_Possible')).alias('wrong_points'))
incorrect_option_perc = perc_correct_ans.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID',\
'Position_Number'],how="left")
# incorrect_option_perc = incorrect_option_perc.withColumn('Incorrect_Choices_Percentage',
#     concat(
#         round((col('wrong_points') / col('Total_points')) * 100, 2).cast("string"),
#         lit('%')
#     )
# )
# results = incorrect_option_perc.withColumn("Result", concat(col("Incorrect_Choices_Percentage"), lit(" chose ["), col("Answer_Submission"), lit("]")))
# grouped_result = results.groupBy('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number',\
# 'Percentage_Correct_Answers','Performance_Color_By_Question') \
#                 .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
grouped_result = incorrect_option_perc.orderBy('Position_Number')
student_submissions_attempt = student_submissions_df.withColumn(
'assessment_attempt',
date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_attempt[['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID',"assessment_attempt"]]\
.dropDuplicates()
concatenated_df = student_submissions_date.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID')\
.agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
final_attempt = grouped_result.join(concatenated_df,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID'],how='inner')
final_attempt_ins = final_attempt.join(section[["Section_Instructors","Section_NID","School_ID"]],on=["Section_NID","School_ID"])
# final_attempt_ins = final_attempt_ins.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
# join with standarsd to get standard and description
distinct_questiondata_info = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number',\
'Question_No','Question',"Standards"])
question_stand = distinct_questiondata_info.join(standard[["Standards","Description"]],on=["Standards"],how="left")
question_stand = question_stand.fillna({'Standards': 'Others', 'Description': 'Others'})
question_stand = question_stand.drop('Total_Points')
final_attempt_ins = final_attempt_ins.join(question_stand,on=['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# get four header points
four_head = all_submissions.groupby('Session', 'School_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID').\
agg(
        sum("Points_Possible").alias("Total_Possible_Points"),
        countDistinct("User_UID").alias("Total_Students"),
        countDistinct("Question_ID").alias("Total_Questions"),
        sum("Points_Received").alias("Total_Score"),
       
)
final_attempt_ins = final_attempt_ins.join(four_head,on=['Session', 'School_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID'])
final_attempt_ins = final_attempt_ins.withColumn(
"UniqueID",
concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Standards"), col("Session")))
final_attempt_ins = final_attempt_ins.withColumn("ID", monotonically_increasing_id())
# final_attempt_ins.filter((col('Question_ID') == '1906562519') | (col('Question_ID') == '1906587321')).select([

#     'Question_ID',
#     'Total_points','Incorrect_Choices_Percentage',
#     'Percentage_Correct_Answers',
#     'Answer_Submission',
#     'wrong_points',
#     'Section_Instructors',
#     'Correct_Answer',
#     'Total_Possible_Points',
#     'Total_Students',
#     'Total_Questions',
#     'Total_Score'
# ]).show(truncate=False)
schoology.publish(final_attempt_ins,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis',f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis')


# In[ ]:





# In[93]:


# Sapan Sir's standard deep dive 
submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
# questiondata =oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
# standard= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
question_data = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data').alias('qd')  
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard').alias('std')

section= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards").alias('std')
standard = standard.select("Standards", "Description", "cPalms_Standard", "Strand", "Cluster", "lastChangeDateTime")
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
questiondata = question_data.join(standard,on=["Standards"],how="left")
# questiondata_joined = question_data.join(
#     standard,
#     (F.col('qd.Standards') == F.col('std.Standards')) | F.col('qd.Standards').contains(F.col('std.Standards')),
#     how='left'
# )
questiondata_joined = question_data.join(
    standard,
    (F.col('qd.Standards') == F.col('std.Standards')) | F.expr("qd.Standards LIKE CONCAT('%', std.Standards)"),
    how='left'
)

questiondata = questiondata_joined.drop(F.col('std.Standards'))
submissions = submissions.join(questiondata, on=["Question_ID", "Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Item_Name", "Session"], how="inner")
submissions = submissions.join(item,on=["Item_ID","Item_Name","School_ID"])
submissions = submissions.na.drop(subset=["Assessment_type","Section_NID","Session","School_ID","Grade","Subject"])
standard1 = submissions.select("Standards","School_ID","Grade","Subject","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime","subject")
#calculation by standard
calScorePossiblePoint= submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Description","Strand","Session").\
agg(countDistinct("Question_NO").alias("Total_Question_By_Standard"),countDistinct("Standards").alias("Total_Sub_Standard"))

calScorePossiblePointBYSection = submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard","Description","Strand","Section_NID","Session").agg(round(sum("Points_Possible"),2).alias("Possible_PointsByStandard"),round(sum("Points_Received"),2).alias("Score_ByStandard"),countDistinct("User_UID").alias("Total Student"))
TotalScorebyStandardBYSection = calScorePossiblePointBYSection.withColumn("% Total_Score_By_Standard", round((col("Score_ByStandard") / col("Possible_PointsByStandard")) * 100, 2))
TotalScorebyStandardBYSection = TotalScorebyStandardBYSection.withColumn("% Total_Incorrect_Score_By_Standard", round(100 -col("% Total_Score_By_Standard"), 2))
TotalScorebyStandardBYSection =TotalScorebyStandardBYSection.join(calScorePossiblePoint,on=["Assessment_type","School_ID","Grade","Subject","Item_Name","Standards","cPalms_Standard",\
"Description","Strand","Session"],how="left")
TotalScorebyStandard = TotalScorebyStandardBYSection.fillna({'Standards': 'Other'})
Selectedattribute = col("% Total_Score_By_Standard")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
TotalScorebyStandard = TotalScorebyStandard.withColumn("Performance_Color_By_Standard", result)
calScorePossiblePointByStrand = submissions.groupby("Assessment_type","School_ID","Grade","Subject","Item_Name","Strand","Session").agg(round(sum("Points_Possible"),2).\
alias("Possible_PointsByStrand"),round(sum("Points_Received"),2).alias("Score_ByStrand"),countDistinct("Question_NO").alias("Total_Question_By_Strand"),countDistinct("Standards").alias("Total_Sub_Strand"))
TotalScorebyStandardByStrand=calScorePossiblePointByStrand.withColumn("% Total_Score_By_Strand", round((col("Score_ByStrand") / col("Possible_PointsByStrand")) * 100, 2))
TotalScorebyStandardByStrand=TotalScorebyStandardByStrand.withColumn("% Total_Incorrect_Score_By_Strand", round(100-col("% Total_Score_By_Strand"), 2))
TotalScorebyStandardByStrand = TotalScorebyStandardByStrand.fillna({'Strand': 'Other'})
Selectedattribute = col("% Total_Score_By_Strand") 
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
.when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
.when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
.otherwise(None)
)
TotalScorebyStandardByStrand=TotalScorebyStandardByStrand.withColumn("Performance_Color_By_Strand", result)

FinalResult=TotalScorebyStandard.join(TotalScorebyStandardByStrand,on=["Assessment_type","School_ID","Grade","Subject","Item_Name","Strand","Session"],how="left")
FinalResult=FinalResult.join(section,on=["Section_NID","School_ID"])
FinalResult = FinalResult.withColumn(
    "UniqueID",
    concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"), col("Section_NID"), col("Standards"), col("Session"))
)
# FinalResult.filter(col('Standards')=='SOC.7.SS.7.C.2.4').show(truncate=False)
FinalResult = FinalResult.withColumn("ID", monotonically_increasing_id())
schoology.publish(FinalResult,f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive',f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Standard_Deep_Dive')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Standard_Deep_Dive')


# In[41]:


# Suruchi's agg_question_response_analysis for column Incorrect_Choice_Details

section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
# submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard = oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
# question_df = question_df.join(standard,on=["Standards"],how="left")
distinct_questiondata = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# distinct_questiondata = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Question_No','Question',\
# 'Position_Number','Correct_Answer','Standards'])
distinct_questiondata = distinct_questiondata.select('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Question_ID','Position_Number')
submissions = student_submissions_df.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID',\
'Position_Number','Answer_Submission','Points_Received','Points_Possible'])
submissions = submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade',\
'Section_NID','Question_ID','Position_Number','Answer_Submission','Points_Received','Points_Possible')
submissions_questions = submissions.join(distinct_questiondata,on=['Session', 'School_ID','Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
# student_submissions_df = student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
all_submissions = submissions_questions.join(item_df,on=["item_ID","Item_Name","School_ID"])
perc_correct_ans = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number').agg(\
    sum(col('Points_Possible')).alias('Total_points'),
    round(sum(col('Points_Received')) / sum(col('Points_Possible')), 2).alias('Percentage_Correct_Answers')
)
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
when(Selectedattribute/100 >= High, "#99FF99") \
    .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
    .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
    .otherwise(None)
)
perc_correct_ans = perc_correct_ans.withColumn("Performance_Color_By_Question", result)
# perc_correct_ans = perc_correct_ans.withColumn('Percentage_of_Correct_Answers',               # % age sign
#     concat(col('Percentage_Correct_Answers').cast("string"), lit('%'))
# )
# perc_correct_ans = perc_correct_ans.drop("Percentage_Correct_Answers") 
# Incorrect Choice Details
incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]
# incorrect_submissions = incorrect_submissions.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# 'Points_Received','Points_Possible','Answer_Submission'])
# incorrect_submissions = incorrect_submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# 'Points_Received','Points_Possible','Answer_Submission')
Incorrect_Choices = incorrect_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number',\
'Answer_Submission').agg(
    sum(col('Points_Possible')).alias('wrong_points'))
incorrect_option_perc = perc_correct_ans.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID',\
'Position_Number'],how="left")

# incorrect_option_perc.filter(col('Question_ID')=='1906562519').show(truncate=False)
# incorrect_option_perc = incorrect_option_perc.withColumn('Incorrect_Choices_Percentage',
#     concat(
#         round((col('wrong_points') / col('Total_points')) * 100, 2).cast("string"),
#         lit('%')
#     )
# )
# results = incorrect_option_perc.withColumn("Result", concat(col("Incorrect_Choices_Percentage"), lit(" chose ["), col("Answer_Submission"), lit("]")))
# grouped_result = results.groupBy('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number',\
# 'Percentage_Correct_Answers','Performance_Color_By_Question') \
#                 .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
grouped_result = incorrect_option_perc.orderBy('Position_Number')
print("grouped_result.columns---",grouped_result.columns)
student_submissions_attempt = student_submissions_df.withColumn(
'assessment_attempt',
date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_attempt[['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID',"assessment_attempt"]]\
.dropDuplicates()
concatenated_df = student_submissions_date.groupby('Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID')\
.agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
final_attempt = grouped_result.join(concatenated_df,on=['Session', 'School_ID', 'Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID'],how='inner')
final_attempt_ins = final_attempt.join(section[["Section_Instructors","Section_NID","School_ID"]],on=["Section_NID","School_ID"])
print("\nfinal_attempt_ins.columns-----------",final_attempt_ins.columns)
# final_attempt_ins = final_attempt_ins.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
# join with standarsd to get standard and description
distinct_questiondata_info = question_df.dropDuplicates(['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number',\
'Question_No','Question',"Standards"])
question_stand = distinct_questiondata_info.join(standard[["Standards","Description"]],on=["Standards"],how="left")
question_stand = question_stand.fillna({'Standards': 'Others', 'Description': 'Others'})
print(question_stand.columns)
question_stand = question_stand.drop('Total_Points')
final_attempt_ins = final_attempt_ins.join(question_stand,on=['Session', 'School_ID', 'Item_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])
print("\nafter joining with question_stand final_attempt_ins become--",final_attempt_ins.columns)
# get four header points
four_head = all_submissions.groupby('Session', 'School_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID').\
agg(
        sum("Points_Possible").alias("Total_Possible_Points"),
        countDistinct("User_UID").alias("Total_Students"),
        countDistinct("Question_ID").alias("Total_Questions"),
        sum("Points_Received").alias("Total_Score"),
        round((sum("Points_Received") / sum("Points_Possible")) * 100, 2).alias("Grade_Average")
)
final_attempt_ins = final_attempt_ins.join(four_head,on=['Session', 'School_ID','Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID'])
final_attempt_ins = final_attempt_ins.withColumn(
"UniqueID",
concat_ws("_", col("Assessment_type"), col("School_ID"), col("Grade"), col("Subject"), col("Item_Name"),col("Section_NID"), col("Standards"), col("Session")))
final_attempt_ins = final_attempt_ins.withColumn("ID", monotonically_increasing_id())
# final_attempt_ins.filter(col('Question_ID')=='1906562519').show(truncate=False)
final_attempt_ins.filter(col('Question_ID')=='1906562518').show(truncate=False)
# schoology.publish(final_attempt_ins,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis',f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis')


# In[ ]:


#  Sapan Sir's agg_question_response_analysis_By_All
# But I am doing changes and making it same to ag_ques_resp_ana excluding Section_NID Column

section =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_section')
question_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_question_data')
item_df =  oea.load(f'stage2/Enriched/schoology/v{schoology.version}/dim_item')
submissions_df = oea.load(f'stage2/Enriched/schoology/v{schoology.version}/fact_student_submission')
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
standard = oea.load(f'stage3/Published/schoology/v0.1/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard=standard.select("Standards","Description","cPalms_Standard","Strand","Cluster","lastChangeDateTime")
question_df=question_df.join(standard,on=["Standards"],how="left")
student_submissions_df=student_submissions_df.join(question_df,["Question_Id","Assessment_type","Correct_Answer","School_ID","Grade","Subject","Item_ID","Item_Name","Session"])
student_submissions_df=student_submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
submissions_df=submissions_df.join(item_df,on=["item_Id","Item_Name","School_ID"])
DataByQuestion=student_submissions_df.select("Assessment_type","Question_No","School_ID","Item_Name","Correct_Answer","Grade","Subject","Standards","cPalms_Standard","Session",\
"Points_Possible","Points_Received")
CalDataByQuestion=student_submissions_df.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject",\
"Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question"),sum("Points_Received").alias("% Total_Correct_Answer_By_Question"))
CalDataByQuestion=CalDataByQuestion.withColumn("Percentage_Correct_Answers", round((col("% Total_Correct_Answer_By_Question") / col("% Total_Possible_Point_By_Question")) * 100, 2))
studentsubmission_Incorrect=student_submissions_df.filter(col("Points_Received")<col("Points_Possible"))
CalDataByQuestionByAnswer=studentsubmission_Incorrect.groupby("Assessment_type","Question_No","Correct_Answer","Question","School_ID","Answer_Submission","Item_Name","Grade","Standards",\
"cPalms_Standard","Subject","Session").agg(sum("Points_Possible").alias("% Total_Possible_Point_By_Question_ByAnswer"),sum("Points_Received").\
alias("% Total_Correct_Answer_By_Question_ByAnswer"))
CalDataByQuestionByAnswer=CalDataByQuestionByAnswer.join(CalDataByQuestion,on=["Assessment_type","Question_No","Correct_Answer","Question","School_ID","Item_Name",\
"Grade","Standards","cPalms_Standard","Subject","Session"],how="left")
CalDataByQuestion_ByAnswer=CalDataByQuestionByAnswer.withColumn("Percentage_Correct_Answers_ByAnswer", round((col("% Total_Possible_Point_By_Question_ByAnswer") / \
col("% Total_Possible_Point_By_Question")) * 100, 2))
Selectedattribute = col("Percentage_Correct_Answers")
High = 0.8
Low = 0.7
result = (
        when(Selectedattribute/100 >= High, "#99FF99") \
        .when((Selectedattribute/100 < Low) & (Selectedattribute/100 > 0), "#FFCCFF") \
        .when((Selectedattribute/100 > Low) & (Selectedattribute/100 < High), "#fff492") \
        .otherwise(None)
        )
CalDataByQuestion=CalDataByQuestion.withColumn("Total_Performance_Color_By_Question", result)

# Remove duplicate rows based on Answer_Submission
Question_Data_Incorrect = CalDataByQuestion_ByAnswer
results = Question_Data_Incorrect.withColumn("Result", concat(
    format_string("%.2f%%", col("Percentage_Correct_Answers_ByAnswer")), lit(" chose ["), col("Answer_Submission"), lit("]")))
grouped_result = results.groupBy("Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject","Session") \
                        .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))
CalDataByQuestion=CalDataByQuestion.join(grouped_result,on=["Assessment_type","Question_No","Correct_Answer","School_ID","Item_Name","Grade","Standards","cPalms_Standard","Subject",\
"Session"],\
how="left")
student_submissions_df = submissions_df.withColumn(
    'assessment_attempt',
    date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_df[["Assessment_type", "Session", "School_ID", "Grade", "Subject", "Item_Name","assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby("Assessment_type","School_ID","Item_Name","Grade","Subject","Session").agg(concat_ws(",", collect_list("assessment_attempt")).\
alias("assessment_date"))
CalDataByQuestion=CalDataByQuestion.join(concatenated_df,on=["Assessment_type","School_ID","Item_Name","Grade","Subject","Session"],how='inner')
Total_Possible_Points=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Possible").alias("Total_Possible_Point"))
Total_Possible_Received=student_submissions_df.groupBy("School_ID","Item_Name","Grade","Subject","Assessment_type","Session").agg(sum("Points_Received").alias("Total_Correct_Answer"))
TotalTable_Join=Total_Possible_Points.join(Total_Possible_Received,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"],how="inner")
TotalTable_Join=TotalTable_Join.withColumn("Total_Score", round((col("Total_Correct_Answer") / col("Total_Possible_Point")) * 100, 2))
CalDataByQuestion=CalDataByQuestion.join(TotalTable_Join,on=["School_ID","Item_Name","Grade","Subject","Assessment_type","Session"])
finalDataByQuestion = CalDataByQuestion.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))
finalDataByQuestion = finalDataByQuestion.withColumn("ID", monotonically_increasing_id())
# schoology.publish(finalDataByQuestion,f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All',f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/agg_Question_Response_Analysis_By_All')


# In[ ]:





# In[31]:


# new aggragate table single using cube
# agg_Total_Student
submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')

standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards", "Description", "cPalms_Standard", "Strand", "Cluster", "lastChangeDateTime")

# total_students_ques = total_students_ques.na.drop(subset=["Assessment_ID", "School_ID", "Grade_ID",'Item_ID,' "Subject_ID", "Section_NID", "Session"])
# Total_Score and Total_Possible_Point
unique_ques_pts = submissions[['User_UID', "Assessment_ID", "School_ID", "Grade_ID","Item_ID","Subject_ID", "Section_NID", "Session","Question_ID","Points_Received","Points_Possible"]].dropDuplicates()
# Test_taken
test_taken = submissions.cube("User_UID","Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session").\
agg(countDistinct("Item_ID").alias("Test_Taken"))
unique_ques_pts_testTaken = unique_ques_pts.join(test_taken,on=["User_UID","Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session"])
total_students_ques = unique_ques_pts_testTaken.cube("Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session").agg(
    countDistinct("User_UID").alias("Total_Students"),
    countDistinct("Question_ID").alias("Total_Questions"),
    sum('Points_Received').alias("Total_Score"),
    sum("Points_Possible").alias("Total_Possible_Point"),
    countDistinct("Item_ID").alias("Test_Taken"),
    round((sum("Points_Received") / sum("Points_Possible")) * 100, 2).alias("Grade_Average")
)
total_students_ques.filter(
    (col('Grade_ID') == 'ca39914f4c46de3c0d25a69aaa4f3c3677d62f3c722012429e943be2649f69a6') &     # Grade 1
    (col('Subject_ID') == '55d6ddd2dae7408666d55af0d9e10c4e850f65776326280737be918ce753f2e0') &   # Math
    (col('Item_ID') == '6772000233')                                                              # Chapter 1
).show()


# In[4]:


# new aggragate table single using cube
# agg_Total_Student
submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')

standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards", "Description", "cPalms_Standard", "Strand", "Cluster", "lastChangeDateTime")

# total_students_ques = total_students_ques.na.drop(subset=["Assessment_ID", "School_ID", "Grade_ID",'Item_ID,' "Subject_ID", "Section_NID", "Session"])
# Total_Score and Total_Possible_Point
unique_ques_pts = submissions[['User_UID', "Assessment_ID", "School_ID", "Grade_ID","Item_ID","Subject_ID", "Section_NID", "Session","Question_ID","Points_Received","Points_Possible"]].dropDuplicates()
# Test_taken
test_taken = submissions.cube("User_UID","Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session").\
    agg(countDistinct("Item_ID").alias("Test_Taken"))
unique_ques_pts_testTaken = unique_ques_pts.join(test_taken,on=["User_UID","Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session"])
total_students_ques = unique_ques_pts_testTaken.cube("Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session").agg(
    countDistinct("User_UID").alias("Total_Students"),
    countDistinct("Question_ID").alias("Total_Questions"),
    sum('Points_Received').alias("Total_Score"),
    sum("Points_Possible").alias("Total_Possible_Point"),
    countDistinct("Item_ID").alias("Test_Taken"),
    round((sum("Points_Received") / sum("Points_Possible")) * 100, 2).alias("Grade_Average")
)
total_students_ques.filter(
    (col('Grade_ID') == 'ca39914f4c46de3c0d25a69aaa4f3c3677d62f3c722012429e943be2649f69a6') &     # Grade 1
    (col('Subject_ID') == '55d6ddd2dae7408666d55af0d9e10c4e850f65776326280737be918ce753f2e0') &   # Math
    (col('Item_ID') == '6772000233')                                                              # Chapter 1
).show(2)


# Total Standards, total substandards

# Total_Standards
question_data = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data').alias('qd')  
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard').alias('std')
section= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards").alias('std')
standard = standard.select("Standards","cPalms_Standard")
all_submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  

distinct_question_stand = question_data[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID', 'Standards']].dropDuplicates()
submissions_ques = all_submissions[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID','Section_NID']].dropDuplicates()
distinct_question_sectionNID = distinct_question_stand.join(submissions_ques,on=["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID'])
questiondata_standards = distinct_question_sectionNID.join(
    standard,
    (F.col('qd.Standards') == F.col('std.Standards')) | F.expr("qd.Standards LIKE CONCAT('%', std.Standards)"),
    how='left'
)
questiondata_standards = questiondata_standards.drop(F.col('std.Standards'))
total_stand_subStand = questiondata_standards.cube("Assessment_type", "School_ID", "Grade", "Subject","Section_NID","Item_ID", "Session").agg(
    countDistinct("Standards").alias("Total_Standards"),
    countDistinct("cPalms_Standard").alias("Total_Sub_Standards")
)

student_submissions_attempt = all_submissions.withColumn(
'assessment_attempt',
date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_attempt[['Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade','Section_NID',"assessment_attempt"]].dropDuplicates()
student_submissions_date = student_submissions_date.cube('Session', 'School_ID', 'Item_ID','Assessment_type', 'Subject', 'Grade','Section_NID')\
.agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
total_stand_subStand = total_stand_subStand.join(student_submissions_date,on=['Session', 'School_ID', 'Item_ID','Assessment_type', 'Subject', 'Grade','Section_NID'])

total_stand_subStand.filter(
    (F.col('Item_ID') == '6772003028')).show()

# distinct_questiondata = question_data[['Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number']].dropDuplicates()
distinct_questiondata = question_data[['Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number']].dropDuplicates()
submissions = all_submissions[['Session', 'School_ID', 'User_UID','Item_ID', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID',\
'Position_Number','Answer_Submission','Points_Received','Points_Possible']].dropDuplicates()
submissions_questions = submissions.join(distinct_questiondata,on=['Session', 'School_ID','Item_ID', 'Assessment_type', 'Subject', 'Grade','Question_ID','Position_Number'])

# submissions_questions.show()
perc_correct_ans = submissions_questions.cube('Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number').agg(\
    sum(col('Points_Possible')).alias('Total_point'),
    round((sum(col('Points_Received')) / sum(col('Points_Possible'))) * 100, 2).alias('Percentage_Correct_Answers')
)
perc_correct_ans = perc_correct_ans.withColumn(
    'Percentage_Incorrect_Answers',
    F.round(100 - F.col('Percentage_Correct_Answers'), 2)
)
# Incorrect Choice Details
incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]

incorrect_submissions = incorrect_submissions[['Session', 'School_ID', 'User_UID','Item_ID', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
'Points_Received','Points_Possible','Answer_Submission']].dropDuplicates()
Incorrect_Choices = incorrect_submissions.cube('Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number','Answer_Submission').agg(
    sum(col('Points_Possible')).alias('wrong_points'))
incorrect_option_perc = perc_correct_ans.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID','Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number'],how="left")

question_info = total_stand_subStand.join(incorrect_option_perc,on=['Session', 'School_ID', 'Item_ID', 'Assessment_type','Section_NID','Subject', 'Grade'])
# joining with QuestionIDS to get Standard Value 
question_info = question_info.join(distinct_question_sectionNID,on=["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID",'Section_NID', "Session",'Question_ID'])

dim_assessment_type = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_assessment_type')
dim_grade = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_grade')
dim_subject = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_subject')
dim_assessment_type = dim_assessment_type.dropna(subset=["Assessment_ID", "School_ID", "Assessment_type"])     # Remove rows with null values 
dim_grade = dim_grade.dropna(subset=["Grade_ID", "School_ID", "Grade"])
dim_subject = dim_subject.dropna(subset=["Subject_ID", "School_ID", "Subject"])

#Studnets data for each question
students_data_for_ques = all_submissions[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID','Section_NID','User_UID']].dropDuplicates()
question_info_students = question_info.join(students_data_for_ques,on=["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID','Section_NID'])
whole_data = question_info_students.join(             # to get assessment_ID,Grade_ID,Subject_ID
    dim_assessment_type,
    on=["Assessment_type", "School_ID"],
    how="left"
).join(
    dim_grade,
    on=["Grade", "School_ID"],
    how="left"
).join(
    dim_subject,
    on=["Subject", "School_ID"],
    how="left"
)
whole_data = whole_data.drop("Assessment_type", "Grade", "Subject")

final = total_students_ques.join(whole_data,on=["Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session"])
# session_ID from Session
session = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_session')
final = final.join(session,on=["School_ID","Session"],how="left")    # Session_ID
final = final.drop("Session")

final.filter(
    (col('Grade_ID') == 'ca39914f4c46de3c0d25a69aaa4f3c3677d62f3c722012429e943be2649f69a6') &     # Grade 1
    (col('Subject_ID') == '55d6ddd2dae7408666d55af0d9e10c4e850f65776326280737be918ce753f2e0') &   # Math
    (col('Item_ID') == '6772000233')                                                              # Chapter 1
).show()

partitioned_df = final.repartition("School_ID")


# In[5]:


partitioned_df = final.repartition("School_ID")
partitioned_df.show()


# In[6]:


partitioned_df_filtered_count = partitioned_df.count()
print(f'Number of rows in partitioned_df with Question_ID = 1862156363: {partitioned_df_filtered_count}')

final_filtered_count = final.count()
print(f'Number of rows in final with Question_ID = 1862156363: {final_filtered_count}')


# In[60]:


final = final.withColumn("ID", monotonically_increasing_id())
schoology.publish(final,f'stage2/Enriched/schoology/v{schoology.version}/final_aggregate',f'stage3/Published/schoology/v{schoology.version}/final_aggregate', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/final_aggregate')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/final_aggregate')


# In[54]:


fact_Student_Submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission') 
withColumnRenamed('User_School_ID','School_ID').withColumnRenamed('User_School_Name','School_Name')

dim_grade = fact_Student_Submissions[['Grade_ID','School_ID','Grade']].drop_duplicates()
dim_grade.show()


# In[62]:


final.filter(
    (col('Question_ID') == '1862156363')  
).show()


# In[63]:


count = final.filter(col('Question_ID') == '1862156363').count()
print(count)


# In[ ]:


# doing again - new aggragate table single using cube
# agg_Total_Student
submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  
questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
section = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')

standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
standard = standard.select("Standards", "Description", "cPalms_Standard", "Strand", "Cluster", "lastChangeDateTime")

# total students



# total_students_ques = total_students_ques.na.drop(subset=["Assessment_ID", "School_ID", "Grade_ID",'Item_ID,' "Subject_ID", "Section_NID", "Session"])
# Total_Score and Total_Possible_Point
# users = submissions[["Assessment_ID", "School_ID", "Grade_ID","Item_ID","Subject_ID", "Section_NID", "Session","Question_ID",'User_UID']].dropDuplicates()
questions = questiondata[["Assessment_type", "School_ID", "Grade","Item_ID","Subject", "Session","Question_ID","Standards"]].dropDuplicates()
submissions_with_ques_user = submissions.select("Assessment_ID", "School_ID", "Grade", "Item_ID", "Subject", "Section_NID", "Session", "Question_ID","User_UID","Points_Received","Points_Possible")\
.dropDuplicates()
submissions_with_quesStand = submissions_with_ques_user.join(questions,on=["Assessment_type", "School_ID", "Grade", "Item_ID", "Subject", "Session", "Question_ID"])

print(submissions_with_quesStand.columns)

# # Test_taken
# test_taken = submissions.cube("User_UID","Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session",'User_UID',).\
# agg(countDistinct("Item_ID").alias("Test_Taken"))
# unique_ques_pts_testTaken = unique_ques_pts.join(test_taken,on=["User_UID","Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session"])
# total_students_ques = unique_ques_pts_testTaken.cube("Assessment_ID", "School_ID", "Grade_ID", "Subject_ID", "Item_ID", "Section_NID", "Session").agg(
#     countDistinct("User_UID").alias("Total_Students"),
#     countDistinct("Question_ID").alias("Total_Questions"),
#     sum('Points_Received').alias("Total_Score"),
#     sum("Points_Possible").alias("Total_Possible_Point"),
#     countDistinct("Item_ID").alias("Test_Taken"),
#     round((sum("Points_Received") / sum("Points_Possible")) * 100, 2).alias("Grade_Average")
# )
# total_students_ques.filter(
#     (col('Grade_ID') == 'ca39914f4c46de3c0d25a69aaa4f3c3677d62f3c722012429e943be2649f69a6') &     # Grade 1
#     (col('Subject_ID') == '55d6ddd2dae7408666d55af0d9e10c4e850f65776326280737be918ce753f2e0') &   # Math
#     (col('Item_ID') == '6772000233')                                                              # Chapter 1
# ).show(2)


# # Total Standards, total substandards

# # Total_Standards
# question_data = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data').alias('qd')  
# standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard').alias('std')
# section= oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_section')
# item = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_item')
# standard = standard.withColumnRenamed("Schoology_Standard", "Standards").alias('std')
# standard = standard.select("Standards","cPalms_Standard")
# all_submissions = oea.load(f'stage3/Published/schoology/v{schoology.version}/fact_student_submission')  

# distinct_question_stand = question_data[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID', 'Standards']].dropDuplicates()
# submissions_ques = all_submissions[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID','Section_NID']].dropDuplicates()
# distinct_question_sectionNID = distinct_question_stand.join(submissions_ques,on=["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID'])
# questiondata_standards = distinct_question_sectionNID.join(
#     standard,
#     (F.col('qd.Standards') == F.col('std.Standards')) | F.expr("qd.Standards LIKE CONCAT('%', std.Standards)"),
#     how='left'
# )
# questiondata_standards = questiondata_standards.drop(F.col('std.Standards'))
# total_stand_subStand = questiondata_standards.cube("Assessment_type", "School_ID", "Grade", "Subject","Section_NID","Item_ID", "Session").agg(
#     countDistinct("Standards").alias("Total_Standards"),
#     countDistinct("cPalms_Standard").alias("Total_Sub_Standards")


# In[ ]:





# In[ ]:





# In[ ]:





# ## Facts Table Code Updated

# In[37]:


# fact table
ing_submissions = oea.load(f'stage2/Ingested/schoology/v0.1/student_submissions') 
ing_submissions = ing_submissions.withColumnRenamed('User_School_ID','School_ID').withColumnRenamed('User_School_Name','School_Name')
questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
dim_session = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_session')

unique_submissions = ing_submissions[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID','Section_NID','User_UID',"First_Access","Latest_Attempt","Total_Time",\
    "Submission_Grade","Submission"]].dropDuplicates()
from pyspark.sql.functions import sha2, concat_ws, col
def generate_uuid(*columns):
    combined_column = concat(*[col(column) for column in columns])
    uuid = sha2(combined_column, 256) 
    return uuid
ing_submissions_subID = ing_submissions.withColumn("submission_ID",generate_uuid("User_UID","First_Access","Latest_Attempt","Total_Time","Submission_Grade","Submission"))   # submission_ID will come
ing_submissions_subID = ing_submissions_subID.drop("First_Access","Latest_Attempt","Total_Time","Submission_Grade","Submission")      # remove cols on whcih submission_ID is generated
# submission_answers
submission_answers = ing_submissions_subID[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID','Section_NID','User_UID','submission_ID','Answer_Submission',\
    'Points_Received','Points_Possible']].dropDuplicates()
ques_stand = questiondata[["Question_ID","Standards"]].dropDuplicates()
submission_answers_stand = submission_answers.join(ques_stand,on=["Question_ID"])       # standards will come
submission_answers_stand_cpalms = submission_answers_stand.join(standard["Standards","cpalms_Standard","Identifier"],on=["Standards"])   # cpalms_Standard will come
# to get assessment_ID,Grade_ID,Subject_ID
dim_assessment_type = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_assessment_type')
dim_grade = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_grade')
dim_subject = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_subject')
dim_assessment_type = dim_assessment_type.dropna(subset=["Assessment_ID", "School_ID", "Assessment_type"])     # Remove rows with null values 
dim_grade = dim_grade.dropna(subset=["Grade_ID", "School_ID", "Grade"])
dim_subject = dim_subject.dropna(subset=["Subject_ID", "School_ID", "Subject"])

final_fact = submission_answers_stand_cpalms.join(             
    dim_assessment_type,
    on=["Assessment_type", "School_ID"],
    how="left"
).join(
    dim_grade,
    on=["Grade", "School_ID"],
    how="left"
).join(
    dim_subject,
    on=["Subject", "School_ID"],
    how="left"
).join(
    dim_session,
    on=["Session", "School_ID"],
    how="left"
)
final_fact = final_fact.drop("Assessment_type", "Grade", "Subject","Session","Standards")
final_fact.filter((col('User_UID') == '117456243') & 
# (col('Subject') == 'ELA') &
# (col('Grade') == 'Grade K') &
# (col('Section_NID') == '6771979354') &
(col('Item_ID') == '6771999448')).show()
# schoology.publish(final_fact, f'stage2/Enriched/schoology/v{schoology.version}/final_fact',f'stage3/Published/schoology/v{schoology.version}/final_fact', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/final_fact')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/final_fact')


# In[4]:


# table for aggregate (join final_fact table with incorrect_submissions to get column incorrect_choice_details)
ing_submissions = oea.load(f'stage2/Ingested/schoology/v0.1/student_submissions') 
ing_submissions = ing_submissions.withColumnRenamed('User_School_ID','School_ID').withColumnRenamed('User_School_Name','School_Name')
questiondata = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_question_data')  
standard = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_standard')
standard = standard.withColumnRenamed("Schoology_Standard", "Standards")
dim_session = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_session')

unique_submissions = ing_submissions[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID','Section_NID','User_UID',"First_Access","Latest_Attempt","Total_Time",\
    "Submission_Grade","Submission"]].dropDuplicates()
from pyspark.sql.functions import sha2, concat_ws, col
def generate_uuid(*columns):
    combined_column = concat(*[col(column) for column in columns])
    uuid = sha2(combined_column, 256) 
    return uuid
ing_submissions_subID = ing_submissions.withColumn("submission_ID",generate_uuid("User_UID","First_Access","Latest_Attempt","Total_Time","Submission_Grade","Submission"))   # submission_ID will come
ing_submissions_subID = ing_submissions_subID.drop("First_Access","Latest_Attempt","Total_Time","Submission_Grade","Submission")      # remove cols on whcih submission_ID is generated
# submission_answers
submission_answers = ing_submissions_subID[["Assessment_type", "School_ID", "Grade", "Subject", "Item_ID", "Session",'Question_ID','Section_NID','User_UID','submission_ID','Answer_Submission',\
    'Points_Received','Points_Possible']].dropDuplicates()
ques_stand = questiondata[["Question_ID","Standards"]].dropDuplicates()
submission_answers_stand = submission_answers.join(ques_stand,on=["Question_ID"])       # standards will come
submission_answers_stand_cpalms = submission_answers_stand.join(standard["Standards","cpalms_Standard","Identifier"],on=["Standards"])   # cpalms_Standard will come
# incorrect_choice_details
student_submissions_df = oea.load(f'stage3/Published/schoology/v0.1/fact_student_submission')
all_submissions = student_submissions_df[['Session', 'School_ID', 'User_UID','Item_ID', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID',\
'Position_Number','Answer_Submission','Points_Received','Points_Possible']].dropDuplicates()

# Incorrect answers
incorrect_submissions = all_submissions[all_submissions['Points_Received'] < all_submissions['Points_Possible']]
# incorrect_submissions = incorrect_submissions.dropDuplicates(['Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# 'Points_Received','Points_Possible','Answer_Submission'])
# incorrect_submissions = incorrect_submissions.select('Session', 'School_ID', 'User_UID','User_Role_ID','Item_ID', 'Item_Name', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID','Position_Number',\
# 'Points_Received','Points_Possible','Answer_Submission')
Incorrect_Choices = incorrect_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number','Answer_Submission').agg(
    sum(col('Points_Possible')).alias('wrong_points'))
total_pts = all_submissions.groupby('Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number').agg(\
    sum(col('Points_Possible')).alias('Total_points'))

incorrect_option_perc = total_pts.join(Incorrect_Choices,on=['Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number'],how="left")
incorrect_option_perc = incorrect_option_perc.withColumn('Incorrect_Choices_Percentage',
    concat(
        round((col('wrong_points') / col('Total_points')) * 100, 2).cast("string"),
        lit('%')
    )
)
results = incorrect_option_perc.withColumn("Result", concat(col("Incorrect_Choices_Percentage"), lit(" chose ["), col("Answer_Submission"), lit("]")))

grouped_result = results.groupBy('Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade', 'Section_NID','Question_ID','Position_Number') \
                .agg(concat_ws(", ", collect_list("Result")).alias("Incorrect_Choice_details"))

grouped_result = grouped_result.orderBy('Position_Number')
student_submissions_attempt = student_submissions_df.withColumn(
'assessment_attempt',
date_format(col('Latest_Attempt'), 'MM/dd/yyyy'))
student_submissions_date = student_submissions_attempt[['Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade','Section_NID',"assessment_attempt"]].dropDuplicates()
concatenated_df = student_submissions_date.groupby('Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade','Section_NID')\
.agg(concat_ws(",", collect_list("assessment_attempt")).alias("assessment_date"))
final_incorrect_attempt = grouped_result.join(concatenated_df,on=['Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade','Section_NID'],how='inner')
# final_attempt_ins = final_attempt.join(section[["Section_Instructors","Section_NID","School_ID"]],on=["Section_NID","School_ID"])
final_incorrect_attempt = final_incorrect_attempt.withColumn("Incorrect_Choice_details", substring(col("Incorrect_Choice_details"), 1, 8000))

def concatenate_position_number(position_number, incorrect_choice_details):
    if position_number == "n/a":
        return incorrect_choice_details
    return f"({position_number}) {incorrect_choice_details}"

# Register UDF
concatenate_position_number_udf = udf(concatenate_position_number, StringType())

# Update Incorrect_Choice_details column
final_incorrect_attempt = final_incorrect_attempt.withColumn("Incorrect_Choice_details_posit", concatenate_position_number_udf(col("Position_Number"), col("Incorrect_Choice_details")))
final_incorrect_attempt = final_incorrect_attempt.drop("Incorrect_Choice_details","Position_Number")
# final_attempt.filter((col('Session') =='2023-24') & (col('Question_ID')=='1862152723')).show(5,truncate=False)

whole = submission_answers_stand_cpalms.join(final_incorrect_attempt,on=['Session', 'School_ID', 'Item_ID', 'Assessment_type', 'Subject', 'Grade','Section_NID','Question_ID'])
# to get assessment_ID,Grade_ID,Subject_ID
dim_assessment_type = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_assessment_type')
dim_grade = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_grade')
dim_subject = oea.load(f'stage3/Published/schoology/v{schoology.version}/dim_subject')
dim_assessment_type = dim_assessment_type.dropna(subset=["Assessment_ID", "School_ID", "Assessment_type"])     # Remove rows with null values 
dim_grade = dim_grade.dropna(subset=["Grade_ID", "School_ID", "Grade"])
dim_subject = dim_subject.dropna(subset=["Subject_ID", "School_ID", "Subject"])

final_agg = whole.join(             
    dim_assessment_type,
    on=["Assessment_type", "School_ID"],
    how="left"
).join(
    dim_grade,
    on=["Grade", "School_ID"],
    how="left"
).join(
    dim_subject,
    on=["Subject", "School_ID"],
    how="left"
).join(
    dim_session,
    on=["Session", "School_ID"],
    how="left"
)
final_agg = final_agg.drop("Assessment_type", "Grade", "Subject","Session","Standards")
final_agg.filter((col('User_UID') == '117456243') & (col('Item_ID') == '6771999448')).show()
# schoology.publish(final, f'stage2/Enriched/schoology/v{schoology.version}/final_agg',f'stage3/Published/schoology/v{schoology.version}/final_agg', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/final_agg')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/final_agg')


# In[5]:


# removed User_UID and keeps Question_ID 
quest_cube = final_agg.cube('School_ID', 'Item_ID', 'Section_NID',"Identifier",'Question_ID', 'Assessment_ID', 'Grade_ID', 'Subject_ID', 'session_ID').\
agg(    
    (sum(col('Points_Received')) / sum(col('Points_Possible'))).alias('Percentage_Correct_Answers_by_Question'),
    ((1 - (sum(col('Points_Received')) / sum(col('Points_Possible'))))).alias('Percentage_InCorrect_Answers_by_Question')
)
# to get column Incorrect Choice Details
quest_cube = quest_cube.join(final_agg[['School_ID', 'Item_ID', 'Section_NID',"Identifier",'Question_ID', 'Assessment_ID', 'Grade_ID', 'Subject_ID', 'session_ID','assessment_date','Incorrect_Choice_details_posit']],on=['School_ID', 'Item_ID', 'Section_NID',"Identifier",'Question_ID', 'Assessment_ID', 'Grade_ID', 'Subject_ID', 'session_ID'],how='left')  # assessment_date, Incorrect_Choice_details_posi will come 
quest_cube.show()


# In[6]:


# removed Question_ID and keeps User_UID 
user_cube = final_agg.cube('School_ID', 'Item_ID', 'Section_NID','User_UID',"Identifier", 'submission_ID', 'Assessment_ID', 'Grade_ID', 'Subject_ID', 'session_ID').\
agg(    
    (countDistinct("User_UID")).alias("Total_Students"),
    (countDistinct("Question_ID")).alias("Total_Questions"),
    (countDistinct("Identifier")).alias("Total_Standards"),
    (countDistinct("cpalms_Standard")).alias("Total_SubStandards"),
    (sum("Points_Possible")).alias("Total_Possible_Point"),
    (sum('Points_Received')).alias("Total_Score"),
    (sum("Points_Received") / sum("Points_Possible")).alias("Grade_Average"),
   ( sum(col('Points_Received')) / sum(col('Points_Possible'))).alias('Percentage_Correct_Answers'),
    ((1 - (sum(col('Points_Received')) / sum(col('Points_Possible'))))).alias('Percentage_InCorrect_Answers'),
    (countDistinct("Item_ID")).alias("Test_Taken")  
)
user_cube.show(1)


# In[ ]:





# In[18]:


final_cube = user_cube.join(quest_cube,on=['School_ID', 'Item_ID', 'Section_NID',"Identifier",'Assessment_ID', 'Grade_ID', 'Subject_ID', 'session_ID'],how="left")
final_cube.show(1)

# final_cube = final_cube.withColumn("ID", monotonically_increasing_id())
# schoology.publish(final_cube, f'stage2/Enriched/schoology/v{schoology.version}/final_cube',f'stage3/Published/schoology/v{schoology.version}/final_cube', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/final_cube')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/final_cube')


# In[22]:


#partioining cube testing
# final_cube2 = user_cube.join(quest_cube,on=['School_ID', 'Item_ID', 'Section_NID',"Identifier",'Assessment_ID', 'Grade_ID', 'Subject_ID', 'session_ID'],how="left")
# final_cube_part = final_cube2.repartition("School_ID", "Item_ID", "Section_NID", "Identifier", "Assessment_ID", "Grade_ID", "Subject_ID", "session_ID")
# final_cube_part = final_cube_part.withColumn("ID", monotonically_increasing_id())
# final_cube_part.show(1)


# In[ ]:


# schoology.publish(final_cube_part, f'stage2/Enriched/schoology/v{schoology.version}/final_cube_part',f'stage3/Published/schoology/v{schoology.version}/final_cube_part', primary_key='ID')
# oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/final_cube_part')
# oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/final_cube_part')


# In[ ]:





# ### cubes aggregation (incremental updation)

# In[4]:


# Verifying cubes aggregation (incremental updation)
schema = StructType([
    StructField("School_ID", IntegerType(), True),
    StructField("Item_ID", IntegerType(), True),
    # StructField("Section_NID", IntegerType(), True),
    StructField("Question_ID", IntegerType(), True),
    StructField("User_UID", IntegerType(), True),
    StructField("Answer_Submission", StringType(), True),
    StructField("Points_Received", IntegerType(), True),
    StructField("Points_Possible", IntegerType(), True),
    
    StructField("Identifier", StringType(), True),
    StructField("assessment_date", StringType(), True),
    StructField("Incorrect_Choice_details_posit", StringType(), True),
])
data = [
    (1, 101,  10001, 200, "a. <http://example.com>", 1, 1,  "identifier_1", "01/01/2024", "20.0% chose [b. <http://example.com>]"),
    (1, 101, 10001, 201, "a. <http://example.com>", 1, 1,  "identifier_1", "01/01/2024", "50.0% chose [c. <http://example.com>]"),
    (1, 101,  10001, 202, "b. <http://example.com>", 0, 1, "identifier_2", "02/01/2024", "10.0% chose [a. <http://example.com>]"),
    (1, 101,  10001, 203, "c. <http://example.com>", 1, 1,  "identifier_3", "03/01/2024", "30.0% chose [b. <http://example.com>]")
    ]
old_agg = spark.createDataFrame(data, schema)
existing = old_agg.cube('School_ID', 'Question_ID', 'Item_ID', 'User_UID', 'Identifier').agg(\
    countDistinct("User_UID").alias("Total_Students"),
    countDistinct("Question_ID").alias("Total_Questions"),
    countDistinct("Identifier").alias("Total_Standards"),
    # countDistinct("cpalms_Standard").alias("Total_SubStandards"),
    sum("Points_Possible").alias("Total_Possible_Point"),
    sum('Points_Received').alias("Total_Score"),
    sum("Points_Received") / sum("Points_Possible").alias("Grade_Average"),
    sum(col('Points_Received')) / sum(col('Points_Possible')).alias('Percentage_Correct_Answers'),
    countDistinct("Item_ID").alias("Test_Taken")
)
existing = existing.withColumn("ID", monotonically_increasing_id())
schoology.publish(existing, f'stage2/Enriched/schoology/v{schoology.version}/existing',f'stage3/Published/schoology/v{schoology.version}/existing', primary_key='ID')
oea.add_to_lake_db(f'stage2/Enriched/schoology/v{schoology.version}/existing')
oea.add_to_lake_db(f'stage3/Published/schoology/v{schoology.version}/existing')


# In[ ]:


newdata = [
    (1, 101, 10001, 1000, "a. <http://example.com>", 1, 1, "identifier_1", "01/01/2024", "20.0% chose [b. <http://example.com>]"),
    (1, 101, 10001, 1001, "a. <http://example.com>", 1, 1, "identifier_1", "01/01/2024", "50.0% chose [c. <http://example.com>]")
]
newdf = spark.createDataFrame(newdata, schema)
# newdf.show(truncate=False)
updates = newdf.cube('School_ID', 'Question_ID', 'Item_ID', 'User_UID', 'Identifier').agg(\
    countDistinct("User_UID").alias("Total_Students"),
    countDistinct("Question_ID").alias("Total_Questions"),
    countDistinct("Identifier").alias("Total_Standards"),
    # countDistinct("cpalms_Standard").alias("Total_SubStandards"),
    sum("Points_Possible").alias("Total_Possible_Point"),
    sum('Points_Received').alias("Total_Score"),
    sum("Points_Received") / sum("Points_Possible").alias("Grade_Average"),
    sum(col('Points_Received')) / sum(col('Points_Possible')).alias('Percentage_Correct_Answers'),
    countDistinct("Item_ID").alias("Test_Taken")  ,
)
existing_path_stage2 = f'stage2/Enriched/schoology/v{schoology.version}/existing'
existing_path_stage3 = f'stage3/Published/schoology/v{schoology.version}/existing'
existing = oea.load(f'stage3/Published/schoology/v{schoology.version}/existing')


merge_condition = """
existing.School_ID = updates.School_ID AND
existing.Question_ID = updates.Question_ID AND
existing.Item_ID = updates.Item_ID AND
existing.User_UID = updates.User_UID AND
existing.Assessment_ID = updates.Assessment_ID AND
"""
update_expr = {
    "Total_Students": "existing.Total_Students + updates.Total_Students",
    "Total_Questions": "existing.Total_Questions + updates.Total_Questions",
    "wrong_points": "existing.wrong_points + updates.wrong_points",
    "Test_Taken": "existing.Test_Taken + updates.Test_Taken",
    "Grade_Average": "round((existing.Total_Score + updates.Total_Score) / (existing.Total_Possible_Point + updates.Total_Possible_Point) * 100, 2)",
    "Percentage_Correct_Answers": "round((existing.Total_Score + updates.Total_Score) / (existing.Total_Possible_Point + updates.Total_Possible_Point) * 100, 2)"
}
(existing.alias("existing")
 .merge(updates.alias("updates"), merge_condition)
 .whenMatchedUpdate(set=update_expr)
 .whenNotMatchedInsertAll()
 .execute())

