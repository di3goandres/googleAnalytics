/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [id]
      ,[fecha_inicio]
      ,[tabla]
  FROM [analytics].[dbo].[ga_parametros]

  update [analytics].[dbo].[ga_parametros]
  set fecha_inicio =  '2017-01-01'
  
  truncate table [dbo].[ga_indicador_Acquisition]
  truncate table [dbo].[ga_indicador_AllTrafficReferrals]
  truncate table [dbo].[ga_indicador_Audience]
  truncate table [dbo].[ga_indicador_OrganicSearches]
  truncate table dbo.ga_indicador_Pages