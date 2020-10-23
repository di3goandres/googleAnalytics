USE [vue_v12]
GO

/****** Object:  Table [dbo].[ga_indicador_AllTrafficReferrals]    Script Date: 23/10/2020 6:49:42 p. m. ******/
DROP TABLE [dbo].[ga_indicador_AllTrafficReferrals]
GO

/****** Object:  Table [dbo].[ga_indicador_AllTrafficReferrals]    Script Date: 23/10/2020 6:49:42 p. m. ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ga_indicador_AllTrafficReferrals](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[fecha] [date] NOT NULL,
	[country] [varchar](2000) NOT NULL,
	[city] [varchar](2000) NULL,
	[medium] [varchar](2000) NOT NULL,
	[fullReferrer] [varchar](2000) NOT NULL,
	[referralPath] [varchar](2000) NOT NULL,
	[pageviews] [int] NOT NULL,
	[users] [int] NOT NULL,
	[newUsers] [int] NOT NULL,
	[sessions] [int] NOT NULL,
	[fecha_creacion] [datetime] NOT NULL,
	[fecha_actualizacion] [datetime] NOT NULL,
 CONSTRAINT [PK_ga_indicador_AllTrafficReferrals] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


