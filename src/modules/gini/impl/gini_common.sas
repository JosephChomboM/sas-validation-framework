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

%macro _gini_partition_vars(train_data=, oot_data=, vars_num=,
    out_train=_gini_vars_train, out_oot=_gini_vars_oot,
    out_shared=_gini_vars_shared);

    %local _i _var _exists_train _exists_oot _vars_train _vars_oot
        _vars_shared;

    %let _vars_train=;
    %let _vars_oot=;
    %let _vars_shared=;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %do %while(%length(&_var.) > 0);
        %_gini_var_exists(data=&train_data., var=&_var., outvar=_exists_train);
        %_gini_var_exists(data=&oot_data., var=&_var., outvar=_exists_oot);

        %if &_exists_train.=1 %then %let _vars_train=&_vars_train. &_var.;
        %if &_exists_oot.=1 %then %let _vars_oot=&_vars_oot. &_var.;
        %if &_exists_train.=1 and &_exists_oot.=1 %then
            %let _vars_shared=&_vars_shared. &_var.;

        %if &_exists_train.=0 and &_exists_oot.=0 %then %do;
            %put WARNING: [gini_variables] Variable &_var. no existe en TRAIN
                ni en OOT y sera omitida.;
        %end;
        %else %if &_exists_train.=0 %then %do;
            %put WARNING: [gini_variables] Variable &_var. no existe en TRAIN.;
            %put WARNING: [gini_variables] Variable &_var. se calculara solo
                en OOT.;
        %end;
        %else %if &_exists_oot.=0 %then %do;
            %put WARNING: [gini_variables] Variable &_var. no existe en OOT.;
            %put WARNING: [gini_variables] Variable &_var. se calculara solo
                en TRAIN.;
        %end;

        %let _i=%eval(&_i. + 1);
        %let _var=%scan(&vars_num., &_i., %str( ));
    %end;

    %let &out_train=%sysfunc(compbl(&_vars_train.));
    %let &out_oot=%sysfunc(compbl(&_vars_oot.));
    %let &out_shared=%sysfunc(compbl(&_vars_shared.));

%mend _gini_partition_vars;

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
