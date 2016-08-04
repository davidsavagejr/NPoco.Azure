using System;
using System.Data.SqlClient;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

namespace NPoco.SqlAzure
{

    public sealed class SqlAzureTransientDetectionStrategy : ITransientErrorDetectionStrategy
    {
        public bool IsTransient(Exception ex)
        {
            if (ex != null)
            {
                SqlException sqlException;
                if ((sqlException = ex as SqlException) != null)
                {
                    foreach (SqlError error in sqlException.Errors)
                    {
                        switch (error.Number)
                        {
                            case 40501:
                                ThrottlingCondition throttlingCondition = ThrottlingCondition.FromError(error);
                                sqlException.Data[(object)throttlingCondition.ThrottlingMode.GetType().Name] = (object)throttlingCondition.ThrottlingMode.ToString();
                                sqlException.Data[(object)throttlingCondition.GetType().Name] = (object)throttlingCondition;
                                return true;
                            case 40540:
                            case 40613:
                            case 10928:
                            case 10929:
                            case 40143:
                            case 40197:
                            case 233:
                            case 10053:
                            case 10054:
                            case 10060:
                            case 20:
                            case 64:
                                return true;
                            default:
                                continue;
                        }
                    }
                }
                else
                {
                    return ex is TimeoutException;
                }
            }
            return false;
        }
    }
}