import sys
import os
import pandas as pd
import logging as log
import uuid

from highlevel_sdk.models.models import *
from highlevel_sdk.date import *


class GoHighLevelAPI:
    """
    Interface for communication with the GoHighLevel API.
    Responsible for making requests and returning data in a standardized format.
    """
    def __init__(self, token: str, id_location: str):
        self.token_data = {"access_token": token}
        self.id_location = id_location
        self.location_obj = Location(token_data=self.token_data, id=self.id_location)
        self.calendar_obj = Calendar(token_data=self.token_data, id=self.id_location)

    def get_users(self):
        """
        Fetches users data from the API.
        
        Returns:
            list: List of dictionaries containing user data.
        """
        try:
            data_users = self.location_obj.get_users()
            return data_users
        except Exception as e:
            log.error(f"Error fetching users: {e}")
            return []
    
    def get_custom_fields(self):
        """
        Fetches custom fields data from the API.
        
        Returns:
            list: List of dictionaries containing custom fields data.
        """
        try:
            data_cs = self.location_obj.get_custom_fields()
            return data_cs
        except Exception as e:
            log.error(f"Error fetching custom fields: {e}")
            return []
    
    def get_custom_values(self):
        """
        Fetches custom field values from the API.
        
        Returns:
            list: List of dictionaries containing custom field values data.
        """
        data_cv = self.location_obj.get_custom_values()
        try:
            return data_cv
        except Exception as e:
            log.error(f"Error fetching custom_values: {e}")
            return []
    
    def get_calendars_events(self, date, users):
        """
        Fetches calendar events from the API for specified users and date range.
        
        Args:
            date: Reference date for the query
            users: List of user objects containing IDs
            
        Returns:
            list: List of dictionaries containing calendar events data.
        """
        data_calendars = []
        try:
            start_timestamp, end_timestamp = DateUtil.get_next_seven_days_timestamp(date)
            for user in users:
                user_id = user.get('id')
                if user_id:
                    events = self.calendar_obj.get_events(
                        start_date=start_timestamp, 
                        end_date=end_timestamp, 
                        user_id=user_id
                    )
                    if events:
                        data_calendars.extend(events)
                        
            return data_calendars
        except Exception as e:
            log.error(f"Error fetching calendar events: {e}")
            return []

    def get_pipelines(self):
        """
        Fetches pipelines data from the API.
        
        Returns:
            list: List of dictionaries containing pipelines data.
        """
        data_pipelines = self.location_obj.get_pipelines()
        try:
            return data_pipelines
        except Exception as e:
            log.error(f"Error fetching pipelines: {e}")
            return []

    def get_contacts(self):
        """
        Fetches contacts data from the API with a default limit of 100 records.
        
        Returns:
            list: List of dictionaries containing contacts data.
        """
        try:
            contacts_cursor = self.location_obj.get_contacts(limit=100)
            all_contacts = []
            for contact in contacts_cursor:
                all_contacts.append(contact)

            return all_contacts
        except Exception as e:
            log.error(f"Error fetching contacts: {e}")
            return []

    def get_opportunities(self):
        """
        Fetches opportunities data from the API with a default limit of 100 records.
        
        Returns:
            list: List of dictionaries containing opportunities data.
        """
        try:
            data_opportunities = self.location_obj.get_opportunities(limit=100)
            all_opportunities = []
            for opportunity in data_opportunities:
                all_opportunities.append(opportunity)
                
            return all_opportunities
        except Exception as e:
            log.error(f"Error fetching opportunities: {e}")
            return []
    
class UserDataExtractor:
    """
    Responsible for extracting specific data from user objects and returning them in a structured format.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extracts specific user data and organizes it into a dictionary format suitable for DataFrame conversion.

        Returns:
            list: List of dictionaries containing essential user information.
        """
        user_data = []
        for user in self.data:
            user_info = {
                'deleted': user.get('deleted', None),
                'email': user.get('email', None),
                'name': user.get('name', None),
                'id': user.get('id', None),
                'phone': user.get('phone', None)
            }
            user_data.append(user_info)
        return user_data

class CustomFieldsExtractor:
    """
    Responsible for extracting key data from custom fields and returning them in a structured format.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extracts important data from each custom field and organizes them into a dictionary format for DataFrame conversion.

        Returns:
            list: List of dictionaries containing custom fields information.
        """
        custom_fields_data = []

        for field in self.data:
            # Accessing field data
            field_info = {
                'id': field.get('id', None),
                'name': field.get('name', None),
                'data_type': field.get('dataType', None),
                'field_key': field.get('fieldKey', None),
                'picklist_options': field.get('picklistOptions', None),
                'placeholder': field.get('placeholder', None),
                'position': field.get('position', None),
                'standard': field.get('standard', None)
            }
            custom_fields_data.append(field_info)

        return custom_fields_data

class CalendarDataExtractor:
    """
    Responsible for extracting specific data from calendar events and returning them in a structured format.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extracts important data from each calendar event and organizes it into a dictionary format for DataFrame conversion.

        Returns:
            list: List of dictionaries containing calendar events information.
        """
        calendar_data = []

        for event in self.data:
            # Accessing the internal _data dictionary
            event_data = event._data if hasattr(event, '_data') else {}

            # Extracting important data from each event directly from the _data dictionary
            event_info = {
                'event_id': event_data.get('id', None),
                'title': event_data.get('title', None),
                'start_time': event_data.get('startTime', None),
                'end_time': event_data.get('endTime', None),
                'appointment_status': event_data.get('appointmentStatus', None),
                'assigned_user_id': event_data.get('assignedUserId', None),
                'contact_id': event_data.get('contactId', None),
                'address': event_data.get('address', None),
                'created_by_user_id': event_data.get('createdBy', {}).get('userId', None) if event_data.get('createdBy') else None,
                'created_by_source': event_data.get('createdBy', {}).get('source', None) if event_data.get('createdBy') else None
            }

            calendar_data.append(event_info)

        return calendar_data

class CustomValuesExtractor:
    """
    Responsible for extracting key data from custom field values and returning them in a structured format.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extracts important data from each custom value and organizes it into a dictionary format for DataFrame conversion.

        Returns:
            list: List of dictionaries containing custom values information.
        """
        custom_values_data = []

        for field in self.data:
            # Accessing custom value data
            field_info = {
                'id': field.get('id', None),
                'name': field.get('name', None),
                'field_key': field.get('fieldKey', None),
                'value': field.get('value', None),  # May not be present in all cases
            }
            custom_values_data.append(field_info)

        return custom_values_data

class DataFrameFormatter:
    """
    Formats extracted data into a DataFrame for easy visualization and analysis.
    """
    def __init__(self, data_list: list):
        self.data = data_list

    def to_dataframe(self):
        """
        Converts the list of extracted data to a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the structured data.
        """
        return pd.DataFrame(self.data)

class PipelinesExtractor:
    """
    Responsible for extracting key data from pipelines and their stages.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extracts important data from each pipeline and its stages, and organizes it into a dictionary format for DataFrame conversion.

        Returns:
            list: List of dictionaries containing pipeline stages information with references to their parent pipelines.
        """
        pipelines_data = []

        for pipeline in self.data:
            # Extracting pipeline information
            pipeline_info = {
                'pipeline_id': pipeline.get('id', None),
                'pipeline_name': pipeline.get('name', None),
                'date_added': pipeline.get('dateAdded', None),
                'date_updated': pipeline.get('dateUpdated', None)
            }

            # Extracting pipeline stages
            for stage in pipeline.get('stages', []):
                stage_info = {
                    'stage_id': stage.get('id', None),
                    'stage_name': stage.get('name', None),
                    'position': stage.get('position', None),
                    'show_in_funnel': stage.get('showInFunnel', None),
                    'show_in_pie_chart': stage.get('showInPieChart', None),
                    'pipeline_id': pipeline_info['pipeline_id'],  # Adding pipeline ID for reference
                    'pipeline_name': pipeline_info['pipeline_name']  # Adding pipeline name
                }

                pipelines_data.append(stage_info)

        return pipelines_data

class ContactsExtractor:
    """
    Responsible for extracting key data from contacts and organizing them into a structured format.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extracts important data from each contact and organizes it into dictionary formats for DataFrame conversion.

        Returns:
            tuple: 
                - List of dictionaries with contact information
                - List of dictionaries with attribution data
                - List of dictionaries with custom fields data
        """
        contacts_data = []
        attributions_data = []
        custom_fields_data = []

        for contact in self.data:
            # Extracting general contact information
            contact_info = {
                'id': contact.get('id', None),
                'contact_name': contact.get('contactName', None),
                'email': contact.get('email', None),
                'phone': contact.get('phone', None),
                'country': contact.get('country', None),
                'date_added': contact.get('dateAdded', None),
                'date_updated': contact.get('dateUpdated', None),
                'tags': ", ".join(contact.get('tags', [])),
                'source': contact.get('source', None),
            }
            contacts_data.append(contact_info)

            # Processing attributions (if any)
            attributions = contact.get('attributions', [])
            for idx, attribution in enumerate(attributions):
                attribution_info = {
                    'id': uuid.uuid4(),  # Generating unique UUID for each attribution
                    'id_association': contact_info['id'],  # Associating attribution with contact
                    'type': 'Contact',  # Source type
                    'medium': attribution.get('medium', None),
                    'utmSource': attribution.get('utmSource', None),
                    'utmCampaign': attribution.get('utmCampaign', None),
                    'utmContent': attribution.get('utmContent', None),
                    'utmFbclid': attribution.get('utmFbclid', None),
                    'utmSessionSource': attribution.get('utmSessionSource', None),
                    'url': attribution.get('url', None),  # Attribution URL
                }
                attributions_data.append(attribution_info)

            # Processing custom fields (if any)
            custom_fields = contact.get('customFields', [])
            for field in custom_fields:
                field_key = field.get('id', None)
                field_value = field.get('value', None)

                if isinstance(field_value, list):  # If value is a list, convert to string
                    field_value = ", ".join(field_value)

                # Adding custom fields to the separate list
                custom_fields_data.append({
                    'contact_id': contact_info['id'],
                    'field_id': field_key,
                    'field_value': field_value
                })

        return contacts_data, attributions_data, custom_fields_data

class OpportunityExtractor:
    """
    Responsible for extracting key data from opportunities and organizing them into a structured format.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extracts important data from each opportunity and organizes it into dictionary formats for DataFrame conversion.

        Returns:
            tuple:
                - List of dictionaries with opportunity information
                - List of dictionaries with attribution data
        """
        opportunities_data = []
        attributions_data = []

        for opportunity in self.data:
            # Extracting opportunity information
            opportunity_info = {
                'assignedTo': opportunity.get('assignedTo', None),
                'contactId': opportunity.get('contactId', None),
                'createdAt': opportunity.get('createdAt', None),
                'id': opportunity.get('id', None),
                'lastStageChangeAt': opportunity.get('lastStageChangeAt', None),
                'lastStatusChangeAt': opportunity.get('lastStatusChangeAt', None),
                'monetaryValue': opportunity.get('monetaryValue', None),
                'name': opportunity.get('name', None),
                'pipelineId': opportunity.get('pipelineId', None),
                'pipelineStageId': opportunity.get('pipelineStageId', None),
                'pipelineStageUId': opportunity.get('pipelineStageUId', None),
                'status': opportunity.get('status', None),
                'updatedAt': opportunity.get('updatedAt', None),
            }
            opportunities_data.append(opportunity_info)

            # Processing attributions (if any)
            attributions = opportunity.get('attributions', [])
            for idx, attribution in enumerate(attributions):
                attribution_info = {
                    'id': uuid.uuid4(),  # Unique identifier for attribution
                    'id_association': opportunity_info['id'],  # Associating attribution with opportunity
                    'type': 'Opportunity',  # Source type
                    'medium': attribution.get('medium', None),
                    'utmSource': attribution.get('utmSource', None),
                    'utmCampaign': attribution.get('utmCampaign', None),
                    'utmContent': attribution.get('utmContent', None),
                    'utmFbclid': attribution.get('utmFbclid', None),
                    'utmSessionSource': attribution.get('utmSessionSource', None),
                    'url': attribution.get('url', None),  # Attribution URL
                }
                attributions_data.append(attribution_info)

        return opportunities_data, attributions_data

class GoHighLevelService:
    """
    Main service that orchestrates the request, extraction, and formatting of data from GoHighLevel API.
    """
    def __init__(self, token: str, id_location: str):
        self.api = GoHighLevelAPI(token, id_location)
        self.user_list = []
        self.atributions_list = []
        self.custom_field_values_list = []

    def get_users_dataframe(self):
        """
        Retrieves user data and converts it to a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing user data.
        """
        users = self.api.get_users()
        if users:
            extractor = UserDataExtractor(users)
            self.user_list = extractor.extract()
            formatter = DataFrameFormatter(self.user_list)
            return formatter.to_dataframe()
          
        return pd.DataFrame()  # Returns empty DataFrame in case of error
      
    def get_calendars_events_dataframe(self, date):
        """
        Retrieves calendar events and converts them to a DataFrame.

        Args:
            date: Reference date for the query

        Returns:
            pd.DataFrame: DataFrame containing calendar events.
        """
        try:
            calendars = self.api.get_calendars_events(date, self.user_list)
            if calendars:
                extractor = CalendarDataExtractor(calendars)
                extracted_data = extractor.extract()
                formatter = DataFrameFormatter(extracted_data)
                return formatter.to_dataframe()
              
            return pd.DataFrame()  
        except Exception as e:
            log.error(f"Error fetching calendar events: {e}")
            return pd.DataFrame()
    
    def get_custom_fields_dataframe(self):
        """
        Retrieves custom fields data and converts it to a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing custom fields data.
        """
        try:
            custom_fields = self.api.get_custom_fields()

            if custom_fields:
                extractor = CustomFieldsExtractor(custom_fields)
                extracted_data = extractor.extract()
                formatter = DataFrameFormatter(extracted_data)
                return formatter.to_dataframe()
              
            return pd.DataFrame()  
        except Exception as e:
            log.error(f"Error fetching custom fields: {e}")
            return pd.DataFrame()
    
    def get_custom_values_dataframe(self):
        """
        Retrieves custom field values data and converts it to a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing custom field values.
        """
        try:
            custom_values = self.api.get_custom_values()
            if custom_values:
                extractor = CustomValuesExtractor(custom_values)
                extracted_data = extractor.extract()
                formatter = DataFrameFormatter(extracted_data)
                return formatter.to_dataframe()
              
            return pd.DataFrame()  
        except Exception as e:
            log.error(f"Error fetching custom values: {e}")
            return pd.DataFrame()
    
    def get_pipelines_dataframe(self):
        """
        Retrieves pipeline data and converts it to a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing pipeline stages data.
        """
        try:
            pipelines = self.api.get_pipelines()
            if pipelines:
                extractor = PipelinesExtractor(pipelines)
                extracted_data = extractor.extract()
                formatter = DataFrameFormatter(extracted_data)
                return formatter.to_dataframe()
              
            return pd.DataFrame()  
        except Exception as e:
            log.error(f"Error fetching pipelines: {e}")
            return pd.DataFrame()

    def get_contacts_dataframe(self):
        """
        Retrieves contacts data and converts it to a DataFrame.
        Also processes and stores attributions and custom field values.

        Returns:
            pd.DataFrame: DataFrame containing contacts data.
        """
        try:
            contacts = self.api.get_contacts()
            if contacts:
                extractor = ContactsExtractor(contacts)
                extracted_data, atributions_list, self.custom_field_values_list = extractor.extract()
                self._set_atributions(atributions_list)
                formatter = DataFrameFormatter(extracted_data)
                return formatter.to_dataframe()
              
            return pd.DataFrame()  
        except Exception as e:
            log.error(f"Error fetching contacts: {e}")
            return pd.DataFrame()
    
    def get_opportunities_dataframe(self):
        """
        Retrieves opportunities data and converts it to a DataFrame.
        Also processes and stores attributions data.

        Returns:
            pd.DataFrame: DataFrame containing opportunities data.
        """
        try:
            opportunities = self.api.get_opportunities()
            if opportunities:
                extractor = OpportunityExtractor(opportunities)
                extracted_data, atributions_list= extractor.extract()
                self._set_atributions(atributions_list)
                formatter = DataFrameFormatter(extracted_data)
                return formatter.to_dataframe()
              
            return pd.DataFrame()  
        except Exception as e:
            log.error(f"Error fetching opportunities: {e}")
            return pd.DataFrame()

    def get_attributions_dataframe(self):
        """
        Retrieves stored attributions and converts them to a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing attributions data.
        """
        try:
            if self.atributions_list:
                formatter = DataFrameFormatter(self.atributions_list)
                return formatter.to_dataframe()
            
            log.warning("No attributions found.")  
            return pd.DataFrame()  
        except Exception as e:
            log.error(f"Error fetching attributions: {e}")
            return pd.DataFrame()

    def get_custom_field_values_dataframe(self):
        """
        Retrieves stored custom field values and converts them to a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing custom field values data.
        """
        try:
            if self.custom_field_values_list:
                formatter = DataFrameFormatter(self.custom_field_values_list)
                return formatter.to_dataframe()
              
            return pd.DataFrame()  
        except Exception as e:
            log.error(f"Error fetching custom field values: {e}")
            return pd.DataFrame()

    def _set_atributions(self, data):
        """
        Sets or updates attributions data for the object.

        Args:
            data (list): List of attribution dictionaries to be stored.
        """
        if self.atributions_list:
            self.atributions_list.extend(data)
            log.info(f"Attributions list updated: {self.atributions_list}")
        if not self.atributions_list:
            self.atributions_list = data
            log.info(f"Attributions list set: {self.atributions_list}")
