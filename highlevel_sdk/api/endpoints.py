import sys
import os
import pandas as pd
import logging as log
import uuid

from highlevel_sdk.models.models import *
from highlevel_sdk.date import *


class GoHighLevelAPI:
    """
    Interface para comunicação com a API do GoHighLevel.
    Responsável por realizar requisições e retornar dados.
    """
    def __init__(self, token: str, id_location: str):
        self.token_data = {"access_token": token}
        self.id_location = id_location
        self.location_obj = Location(token_data=self.token_data, id=self.id_location)
        self.calendar_obj = Calendar(token_data=self.token_data, id=self.id_location)

    def get_users(self):
        """
        Realiza a requisição à API para buscar usuários.
        
        Returns:
            list: Lista de dicionários com os dados dos usuários.
        """
        try:
            data_users = self.location_obj.get_users()
            return data_users
        except Exception as e:
            log.error(f"Error fetching users: {e}")
            return []
    
    def get_custom_fields(self):
        """
        Realiza a requisição à API para buscar usuários.
        
        Returns:
            list: Lista de dicionários com os dados dos usuários.
        """
        try:
            data_cs = self.location_obj.get_custom_fields()
            return data_cs
        except Exception as e:
            log.error(f"Error fetching users: {e}")
            return []
    
    def get_custom_values(self):
        """
        Realiza a requisição à API para buscar usuários.
        
        Returns:
            list: Lista de dicionários com os dados dos usuários.
        """
        data_cv = self.location_obj.get_custom_values()
        try:
            return data_cv
        except Exception as e:
            log.error(f"Error fetching custom_values: {e}")
            return []
    
    def get_calendars_events(self, date, users):
        """
        Realiza a requisição à API para buscar eventos de calendários.
        
        Returns:
            list: Lista de dicionários com os dados dos eventos dos calendários.
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
        Realiza a requisição à API para buscar pipelines.
        
        Returns:
            list: Lista de dicionários com os dados dos pipelines.
        """
        data_pipelines = self.location_obj.get_pipelines()
        try:
            return data_pipelines
        except Exception as e:
            log.error(f"Error fetching pipelines: {e}")
            return []

    def get_contacts(self):
        """
        Realiza a requisição à API para buscar contatos.
        
        Returns:
            list: Lista de dicionários com os dados dos contatos.
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
        Realiza a requisição à API para buscar oportunidades.
        
        Returns:
            list: Lista de dicionários com os dados das oportunidades.
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
    Responsável por extrair dados específicos de cada usuário e retornar em formato estruturado.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extrai dados específicos e os organiza em formato de dicionário para DataFrame.

        Returns:
            list: Lista de dicionários com as informações necessárias dos usuários.
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
    Responsável por extrair dados importantes dos custom fields e retornar em formato estruturado.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extrai dados importantes de cada campo personalizado e organiza em formato de dicionário para DataFrame.

        Returns:
            list: Lista de dicionários com as informações dos campos personalizados.
        """
        custom_fields_data = []

        for field in self.data:
            # Acessando os dados do campo
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
    Responsável por extrair dados específicos de cada evento de calendário e retornar em formato estruturado.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extrai dados importantes de cada evento e organiza em formato de dicionário para DataFrame.

        Returns:
            list: Lista de dicionários com as informações dos eventos.
        """
        calendar_data = []

        for event in self.data:
            # Acessando o dicionário interno _data
            event_data = event._data if hasattr(event, '_data') else {}

            # Extraindo os dados importantes de cada evento diretamente do dicionário _data
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
    Responsável por extrair dados importantes dos custom values e retornar em formato estruturado.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extrai dados importantes de cada custom value e organiza em formato de dicionário para DataFrame.

        Returns:
            list: Lista de dicionários com as informações dos custom values.
        """
        custom_values_data = []

        for field in self.data:
            # Acessando os dados do custom value
            field_info = {
                'id': field.get('id', None),
                'name': field.get('name', None),
                'field_key': field.get('fieldKey', None),
                'value': field.get('value', None),  # Pode não estar presente em todos os casos
            }
            custom_values_data.append(field_info)

        return custom_values_data

class DataFrameFormatter:
    """
    Formata os dados extraídos em um DataFrame para fácil visualização.
    """
    def __init__(self, data_list: list):
        self.data = data_list

    def to_dataframe(self):
        """
        Converte a lista de dados extraídos para um DataFrame.

        Returns:
            pd.DataFrame: DataFrame contendo os dados dos usuários.
        """
        return pd.DataFrame(self.data)

class PipelinesExtractor:
    """
    Responsável por extrair dados importantes dos pipelines e seus estágios (stages).
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extrai dados importantes de cada pipeline e seus estágios, e organiza em formato de dicionário para DataFrame.

        Returns:
            list: Lista de dicionários com as informações dos pipelines e estágios.
        """
        pipelines_data = []

        for pipeline in self.data:
            # Extraindo informações do pipeline
            pipeline_info = {
                'pipeline_id': pipeline.get('id', None),
                'pipeline_name': pipeline.get('name', None),
                'date_added': pipeline.get('dateAdded', None),
                'date_updated': pipeline.get('dateUpdated', None)
            }

            # Extraindo os estágios (stages) do pipeline
            for stage in pipeline.get('stages', []):
                stage_info = {
                    'stage_id': stage.get('id', None),
                    'stage_name': stage.get('name', None),
                    'position': stage.get('position', None),
                    'show_in_funnel': stage.get('showInFunnel', None),
                    'show_in_pie_chart': stage.get('showInPieChart', None),
                    'pipeline_id': pipeline_info['pipeline_id'],  # Adicionando o ID do pipeline para referência
                    'pipeline_name': pipeline_info['pipeline_name']  # Adicionando o nome do pipeline
                }

                pipelines_data.append(stage_info)

        return pipelines_data

class ContactsExtractor:
    """
    Responsável por extrair dados importantes dos contatos e organizá-los em formato estruturado.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extrai os dados importantes de cada contato e organiza em formato de dicionário para DataFrame.

        Returns:
            tuple: 
                - lista de dicionários com as informações dos contatos.
                - lista de dicionários com as atribuições separadas.
                - lista de dicionários com os campos personalizados (custom fields) separados.
        """
        contacts_data = []
        attributions_data = []
        custom_fields_data = []

        for contact in self.data:
            # Extraindo informações gerais do contato
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

            # Processando as Attributions (caso existam)
            attributions = contact.get('attributions', [])
            for idx, attribution in enumerate(attributions):
                attribution_info = {
                    'id': uuid.uuid4(),  # Gerando UUID único para cada atribuição
                    'id_association': contact_info['id'],  # Associando a atribuição ao contato
                    'type': 'Contact',  # Tipo da origem
                    'medium': attribution.get('medium', None),
                    'utmSource': attribution.get('utmSource', None),
                    'utmCampaign': attribution.get('utmCampaign', None),
                    'utmContent': attribution.get('utmContent', None),
                    'utmFbclid': attribution.get('utmFbclid', None),
                    'utmSessionSource': attribution.get('utmSessionSource', None),
                    'url': attribution.get('url', None),  # URL da atribuição
                }
                attributions_data.append(attribution_info)

            # Processando os Custom Fields (caso existam)
            custom_fields = contact.get('customFields', [])
            for field in custom_fields:
                field_key = field.get('id', None)
                field_value = field.get('value', None)

                if isinstance(field_value, list):  # Caso o valor seja uma lista, converta para string
                    field_value = ", ".join(field_value)

                # Adicionando os custom fields à lista separada
                custom_fields_data.append({
                    'contact_id': contact_info['id'],
                    'field_id': field_key,
                    'field_value': field_value
                })

        return contacts_data, attributions_data, custom_fields_data

class OpportunityExtractor:
    """
    Responsável por extrair dados importantes das oportunidades e organizá-los em formato estruturado.
    """
    def __init__(self, data: list):
        self.data = data

    def extract(self):
        """
        Extrai os dados importantes de cada oportunidade e organiza em formato de dicionário para DataFrame.

        Returns:
            list: Lista de dicionários com as informações das oportunidades.
        """
        opportunities_data = []
        attributions_data = []

        for opportunity in self.data:
            # Extraindo informações da oportunidade
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

            # Processando as Attributions (caso existam)
            attributions = opportunity.get('attributions', [])
            for idx, attribution in enumerate(attributions):
                attribution_info = {
                    'id': uuid.uuid4(),  # Índice da atribuição
                    'id_association': opportunity_info['id'],  # Associando a atribuição à oportunidade
                    'type': 'Opportunity',  # Tipo da origem
                    'medium': attribution.get('medium', None),
                    'utmSource': attribution.get('utmSource', None),
                    'utmCampaign': attribution.get('utmCampaign', None),
                    'utmContent': attribution.get('utmContent', None),
                    'utmFbclid': attribution.get('utmFbclid', None),
                    'utmSessionSource': attribution.get('utmSessionSource', None),
                    'url': attribution.get('url', None),  # URL da atribuição
                }
                attributions_data.append(attribution_info)

        return opportunities_data, attributions_data

class GoHighLevelService:
    """
    Serviço principal que orquestra a requisição, extração e formatação dos dados.
    """
    def __init__(self, token: str, id_location: str):
        self.api = GoHighLevelAPI(token, id_location)
        self.user_list = []
        self.atributions_list = []
        self.custom_field_values_list = []

    def get_users_dataframe(self):
        """
        Obtém os dados dos usuários em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com os dados dos usuários.
        """
        users = self.api.get_users()
        if users:
            extractor = UserDataExtractor(users)
            self.user_list = extractor.extract()
            formatter = DataFrameFormatter(self.user_list)
            return formatter.to_dataframe()
          
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
      
    def get_calendars_events_dataframe(self, date):
        """
        Obtém os eventos dos calendários em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com os eventos dos calendários.
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
        Obtém os campos personalizados em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com os campos personalizados.
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
        Obtém os valores dos campos personalizados em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com os valores dos campos personalizados.
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
            log.error(f"Error fetching custom fields: {e}")
            return pd.DataFrame()
    
    def get_pipelines_dataframe(self):
        """
        Obtém os pipelines em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com os pipelines.
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
        Obtém os contatos em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com os contatos.
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
        Obtém as oportunidades em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com as oportunidades.
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
        Obtém as atribuições em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com as atribuições.
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
        Obtém os valores dos campos personalizados em formato de DataFrame.

        Returns:
            pd.DataFrame: DataFrame com os valores dos campos personalizados.
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
        Define as atribuições para o objeto.

        Args:
            data (list): Lista de atribuições.
        """
        if self.atributions_list:
            self.atributions_list.extend(data)
            log.info(f"Attributions list updated: {self.atributions_list}")
        if not self.atributions_list:
            self.atributions_list = data
            log.info(f"Attributions list set: {self.atributions_list}")
        