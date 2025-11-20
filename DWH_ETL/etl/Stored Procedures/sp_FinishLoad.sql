CREATE   PROCEDURE [etl].[sp_FinishLoad]
  (@LoadID int
  ,@Debug int = 0
)
AS
-- =============================================
-- Author:      solicon-IT, RDTL
-- Create date: 12.11.2025
-- Description: Finish a DWH load  
-- Change Log:
-- Example: 
-- EXEC etl.sp_FinishLoad  4711, 1
-- =============================================
DECLARE
  @ErrorText varchar(4000)

  IF @Debug = 1
    PRINT ('Start Procedure ETL.FinishLoad');

  -- =============================================
  -- Check if load is running
  -- =============================================  
  SELECT TOP 1 @LoadID = LoadID
    FROM etl.Load
   WHERE LoadStatusID = 1 -- Running
     AND LoadID = @LoadID 

  IF @@ROWCOUNT = 0 
  BEGIN
    SET @ErrorText = 'ETL-ERROR: Load cannot be finished because no LoadID: ' +
                     CONVERT(varchar, @LoadID) + ' is running';   
    THROW 51001, @ErrorText, 1;  
  END

  IF @Debug = 1
  BEGIN
    PRINT ('End check if the Load is running');
    PRINT ('Start finishing Load')
  END

  -- =============================================
  -- Finish Load Process
  -- =============================================
  IF @Debug = 1 
    PRINT ('Finishing Load...')

  UPDATE etl.Load
     SET Message = 'Load ' + CONVERT(nvarchar, @LoadID) + ' completed successfully',
         EndDate = GETDATE(),
         LoadStatusID = 2
  WHERE LoadID = @LoadID

  IF @Debug = 1 
  BEGIN
    PRINT ('End Procedure etl.FinishLoad');
  END
