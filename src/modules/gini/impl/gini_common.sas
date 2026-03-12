/* =========================================================================
gini_common.sas - Utilidades reutilizables para Gini con PROC FREQTAB
========================================================================= */

%macro _gini_var_exists(data=, var=, outvar=_gini_exists);
    %local _dsid _rc _exists;
    %let _exists=0;
    %let _dsid=%sysfunc(open(&data.));
    %if &_dsid. > 0 %then %do;
        %if %sysfunc(varnum(&_dsid., &var.)) > 0 %then %let _exists=1;
        %let _rc=%sysfunc(close(&_dsid.));
    %end;
    %let &outvar=&_exists.;
%mend _gini_var_exists;

%macro _gini_freqtab_general(data=, target=, score=, with_missing=1,
    out=work._gini_freqtab);
    %if &with_missing.=1 %then %do;
        proc freqtab data=&data. noprint missing;
            tables &target. * &score. / measures;
            output out=&out. smdcr;
        run;
    %end;
    %else %do;
        proc freqtab data=&data. noprint;
            tables &target. * &score. / measures;
            output out=&out. smdcr;
        run;
    %end;
%mend _gini_freqtab_general;

%macro _gini_freqtab_by(data=, target=, score=, byvar=, with_missing=1,
    out=work._gini_freqtab_by);
    %if &with_missing.=1 %then %do;
        proc freqtab data=&data. noprint missing;
            by &byvar.;
            tables &target. * &score. / measures;
            output out=&out. smdcr;
        run;
    %end;
    %else %do;
        proc freqtab data=&data. noprint;
            by &byvar.;
            tables &target. * &score. / measures;
            output out=&out. smdcr;
        run;
    %end;
%mend _gini_freqtab_by;

%macro _gini_count_rows(data=, target=, score=, with_missing=1,
    outvar=_gini_n);
    %if &with_missing.=1 %then %do;
        proc sql noprint;
            select count(*) into :&outvar trimmed from &data.
                where not missing(&target.);
        quit;
    %end;
    %else %do;
        proc sql noprint;
            select count(*) into :&outvar trimmed from &data.
                where not missing(&target.) and not missing(&score.);
        quit;
    %end;
%mend _gini_count_rows;

%macro _gini_count_rows_by(data=, target=, score=, byvar=, with_missing=1,
    out=work._gini_n_by);
    %if &with_missing.=1 %then %do;
        proc sql noprint;
            create table &out. as
            select &byvar., count(*) as N_Gini
            from &data.
            where not missing(&target.)
            group by &byvar.;
        quit;
    %end;
    %else %do;
        proc sql noprint;
            create table &out. as
            select &byvar., count(*) as N_Gini
            from &data.
            where not missing(&target.) and not missing(&score.)
            group by &byvar.;
        quit;
    %end;
%mend _gini_count_rows_by;
