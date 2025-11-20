CREATE   VIEW [etl].[v_LoadTypeSteps] AS
SELECT lt.LoadTypeID,
       lt.LoadTypeName,
       lts.StepID,
       lts.StepName,
       lts.isActive
  FROM etl.LoadType lt 
  JOIN etl.LoadTypeStep lts 
    ON lt.LoadTypeID = lts.LoadTypeID
