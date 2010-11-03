/*
** Language Query Example Program.
*/

#include <stdio.h>
#include <ctpublic.h>

/*
** Define a global context structure to use
*/
CS_CONTEXT *context;

#define ERROR_EXIT   (-1)
#define        MAXCOLUMNS   2
#define        MAXSTRING    40

extern int print_data();

/* Client message and server message callback routines: */
CS_RETCODE  clientmsg_callback();
CS_RETCODE  servermsg_callback();
void error();

/*
** Main entry point for the program.
*/
main(argc, argv)
int     argc;

char    **argv;
{
  CS_CONNECTION    *connection;   /* Connection structure. */
  CS_COMMAND       *cmd;          /* Command structure.    */

  /* Data format structures for column descriptions: */
  CS_DATAFMT                columns[MAXCOLUMNS];

  CS_INT            datalength[MAXCOLUMNS];
  CS_SMALLINT       indicator[MAXCOLUMNS];
  CS_INT            count;
  CS_RETCODE        ret, res_type;
  CS_CHAR           name[MAXSTRING];
  CS_CHAR           city[MAXSTRING];

  /*
  ** Get a context structure to use.
  */
  cs_ctx_alloc(CS_VERSION_100, &context)

    /*
    ** Initialize Open Client.
    */
    ct_init(context, CS_VERSION_100);

  /*
  ** Install message callback routines.
  */
  ct_callback(context, NULL, CS_SET, CS_CLIENTMSG_CB,
              clientmsg_callback);

  ct_callback(context, NULL, CS_SET, CS_SERVERMSG_CB,
              servermsg_callback);

  /*
  ** Connect to the server:
  **     Allocate a connection structure.
  **     Set user name and password.
  **     Create the connection.
  */
  ct_con_alloc(context, &connection);
  ct_con_props(connection, CS_SET, CS_USERNAME, "username",
               CS_NULLTERM, NULL);
  ct_con_props(connection, CS_SET, CS_PASSWORD, "password",
               CS_NULLTERM, NULL);

  /*
  ** This call actually creates the connection:
  */
  ct_connect(connection, "servername", CS_NULLTERM);

  /*
  ** Allocate a command structure.
  */
  ct_cmd_alloc(connection, &cmd);

  /*
  ** Initiate a language command.
  */
  ct_command(cmd, CS_LANG_CMD,
             "use pubs2 \
              select au_lname, city from pubs2..authors \
                 where state = 'CA'",
             CS_NULLTERM, CS_UNUSED);

  /*
  ** Send the command.
  */
  ct_send(cmd);

  /*
  ** Process the results of the command.
  */
  while((ret = ct_results(cmd, &res_type))== CS_SUCCEED)
    {
      switch (res_type)
        {
        case CS_ROW_RESULT:
          /*
          ** We're expecting exactly two columns.
          ** For each column, fill in the relevant
          ** fields in a data format structure, and
          ** bind the column.
          */
          columns[0].datatype = CS_CHAR_TYPE;
          columns[0].format = CS_FMT_NULLTERM;
          columns[0].maxlength = MAXSTRING;
          columns[0].count = 1;
          columns[0].locale = NULL;
          ct_bind(cmd, 1, &columns[0], name, &datalength[0],
                  &indicator[0]);

          columns[1].datatype = CS_CHAR_TYPE;
          columns[1].format = CS_FMT_NULLTERM;
          columns[1].maxlength = MAXSTRING;
          columns[1].count = 1;
          columns[1].locale = NULL;
          ct_bind(cmd, 2, &columns[1], city, &datalength[1],
                  &indicator[1]);

          /*
          ** Now fetch and print the rows.
          */
          while(((ret = ct_fetch(cmd, CS_UNUSED, CS_UNUSED,
                                 CS_UNUSED, &count))
                 == CS_SUCCEED) || (ret == CS_ROW_FAIL))
            {
              /*
              ** Check if we hit a recoverable error.
              */
              if( ret == CS_ROW_FAIL )
                {
                  fprintf(stderr,
                          "Error on row %d in this fetch batch.",
                          count+1);
                }

              /*
              ** We have a row, let's print it.
              */
              fprintf(stdout, "%s: %s\n", name, city);
            }

          /*
          ** We're finished processing rows, so check
          ** ct_fetch's final return value.
          */
          if( ret == CS_END_DATA )
            {
              fprintf(stdout,
                      "All done processing rows.");
            }
          else /* Failure occurred. */
            {
              error("ct_fetch failed");
            }

          /*
          ** All done with this result set.
          */
          break;

        case CS_CMD_SUCCEED:
          /*
          ** Executed a command that never returns rows.
          */
          fprintf(stderr, "No rows returned.\n");
          break;


        case CS_CMD_FAIL:
          /*
          ** The server encountered an error while
          ** processing our command.
          */
          break;

        case CS_CMD_DONE;
        /*
        ** The logical command has been completely
        ** processed.
        */
        break;

        default:
          /*
          ** We got something unexpected.
          */
          error("ct_result returned unexpected result
                 type");
          break;
        }
    }

  /*
  ** We've finished processing results. Let's check
  ** the return value of ct_results() to see if
  ** everything went ok.
  */
  switch(ret)
    {
    case CS_END_RESULTS:
      /*
      ** Everything went fine.
      */
      break;

    case CS_FAIL:
      /*
      ** Something terrible happened.
      */
      error("ct_results() returned FAIL.");
      break;

    default:
      /*
      ** We got an unexpected return value.
      */
      error("ct_result returned unexpected return
                 code");
      break;
    }

  ** All done.
    */
    ct_cmd_drop(cmd);
  ct_close(connection, CS_UNUSED);
  ct_con_drop(connection);
  ct_exit(context, CS_UNUSED);
  cs_ctx_drop(context);
  return 0;
}

/*
** Error occurred, cleanup and exit.
*/
void error(msg)
     char *msg;
{
  fprintf(stderr, "FATAL ERROR: %s\n", msg);
  exit(ERROR_EXIT);
}
