Append tables when the same variables have different types and lengths

PROBLEM
=======

  proc append base=master data=transaction force;
  run;quit;

  NOTE: Appending WORK.TRANSACTION to WORK.MASTER.
  WARNING: Variable AGE not appended because of type mismatch.
  WARNING: Variable HEIGHT not appended because of type mismatch.
  NOTE: FORCE is specified, so dropping/truncating will occur.


github
https://tinyurl.com/y8r4rwd4
https://github.com/rogerjdeangelis/utl_append_tables_when_the_same_variables_have_different_types_and_lengths

I don't have all my utilities in WPS yet.
Same results in WPS and SAS, however utl_gather was executed on the SAS side.

utl_gather macro from
Alea Iacta
https://github.com/clindocu

* SQL dictionaries are often is often too slow,especially on non-programmers servers;
I need to retype variables in the transaction dataset to match the master

INPUT
=====

WORK.MASTER total obs=4

 Variables in Creation Order

 Variable    Type    Len  |  RULES
                          |
 NAME        Char      8  |  ** same in master and transaction
 WEIGHT      Char      5  |  ** char in master and num in transaction need to convert transaction to car
 HEIGHT      Num       8  |  ** num in master and char in transaction need to convert transaction to num
 AGE         Num       8  |  ** num in master and char in transaction need to convert transaction to num
                          |

WORK.TRANSACTION

 Variables in Creation Order

 Variable    Type    Len

 NAME        Char      8
 WEIGHT      Num       8
 HEIGHT      Char      5
 AGE         Char      5

Types need to match master


PROCESS
=======

  * get meta data only need one ob - sql dictionaaries are two slow;

  %utl_gather(master(obs=1),var,val,,masterXpo,valformat=$8.,WithFormats=Y);

  /*
  WORK.MASTERXPO total obs=4

   VAR       VAL       _COLFORMAT    _COLTYP

   NAME      Alfred     $8.             C
   AGE       14         BEST12.         N
   HEIGHT    69         BEST12.         N
   WEIGHT    112.5      $5.             C
  */

  %utl_gather(transaction(obs=1),var,val,,transactionXpo,valformat=$8.,WithFormats=Y);

  /*
  WORK.TRANSACTIONXPO total obs=4

    VAR       VAL       _COLFORMAT    _COLTYP

    NAME      Alfred     $8.             C
    WEIGHT    112.5      BEST12.         N
    HEIGHT    69.00      $5.             C
    AGE       14         $5.             C
  */

  proc sql;
   select
      case
        when l._coltyp = "N" then catx(" ","input(",l.var,",best12.) as",l.var)
        else catx(" ","put(",l.var,",5.1) as",l.var)
      end as chgTyp
   into
      :varChg  separated by ","
   from
      masterXpo as l, transactionXpo as r
   where
      l.var = r.var  and
      l._coltyp ne r._coltyp
   ;quit;

   /*
   %put &=varChg;

   VARCHG=
         input( AGE ,best12.) as AGE
        ,input( HEIGHT ,best12.) as HEIGHT
        ,put( WEIGHT ,5.1) as WEIGHT
   */

   * retype transaction;
   proc sql;
     create
        table transFix as
     select
        &varChg
       ,*
     from
       transaction
   ;quit;

    * SQL will use the longer lengths. Better than append?;
    proc sql;
      create
        table master as
      select
        *
      from
        master
      outer
        union corr
      select
        *
      from
        transfix
    ;quit;

OUTPUT
======

MASTER total obs=38

  Obs    NAME       AGE    HEIGHT    WEIGHT

    1    Alfred      14     69.0     112.5
    2    Alice       13     56.5     84.00
    3    Barbara     13     65.3     98.00
   17    Ronald      15     67.0     133.0
  ....
   18    Thomas      11     57.5     85.00
   19    William     15     66.5     112.0

   20    Alfred      14     69.0     112.5  ** apended transaction;
   21    Alice       13     56.5      84.0
   22    Barbara     13     65.3      98.0
 ...
   36    Ronald      15     67.0     133.0
   37    Thomas      11     57.5      85.0
   38    William     15     66.5     112.0

*                _               _       _
 _ __ ___   __ _| | _____     __| | __ _| |_ __ _
| '_ ` _ \ / _` | |/ / _ \   / _` |/ _` | __/ _` |
| | | | | | (_| |   <  __/  | (_| | (_| | || (_| |
|_| |_| |_|\__,_|_|\_\___|   \__,_|\__,_|\__\__,_|

;

data transaction;
  set sashelp.class(rename=(age=agen height=heightn));
  height=put(heightn,5.2);
  age=put(agen,5.);
  drop agen heightn sex;
run;quit;


data Master;
  set sashelp.class(rename=(weight=weightn));
  weight=put(weightn,5.2);
  drop sex weightn;
run;quit;

*          _       _   _
 ___  ___ | |_   _| |_(_) ___  _ __
/ __|/ _ \| | | | | __| |/ _ \| '_ \
\__ \ (_) | | |_| | |_| | (_) | | | |
|___/\___/|_|\__,_|\__|_|\___/|_| |_|

;

for SAS see process


* WPS;

* I don't have utl_gather in WPS, so
  I will execute it on the SAS side;

%utl_gather(master(obs=1),var,val,,masterXpo,valformat=$8.,WithFormats=Y);
%utl_gather(transaction(obs=1),var,val,,transactionXpo,valformat=$8.,WithFormats=Y);

%utl_submit_wps64('
  libname wrk sas7bdat "%sysfunc(pathname(work))";
  proc sql;
   select
      case
        when l._coltyp = "N" then catx(" ","input(",l.var,",best12.) as",l.var)
        else catx(" ","put(",l.var,",5.1) as",l.var)
      end as chgTyp
   into
      :varChg  separated by ","
   from
      wrk.masterXpo as l, wrk.transactionXpo as r
   where
      l.var = r.var  and
      l._coltyp ne r._coltyp
   ;quit;
   proc sql;
     create
        table transFix as
     select
        &varChg
       ,*
     from
       wrk.transaction
   ;quit;

    * SQL will use the longer lengths. Better than append?;
    proc sql;
      create
        table master as
      select
        *
      from
        wrk.master
      outer
        union corr
      select
        *
      from
        transfix
    ;quit;
    run;quit;
    proc print;
    run;quit;
');

