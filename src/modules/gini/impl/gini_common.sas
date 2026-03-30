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

%macro _gini_sort_cas(table_name=, orderby=, groupby={});

    %if %length(%superq(table_name))=0 or %length(%superq(orderby))=0 %then
        %return;

    proc cas;
        session conn;
        table.partition /
            table={
                caslib="casuser",
                name="&table_name.",
                orderby=&orderby.,
                groupby=&groupby.
            },
            casout={
                caslib="casuser",
                name="&table_name.",
                replace=true
            };
    quit;

%mend _gini_sort_cas;

%macro _gini_sort_cas_as(source_table=, target_table=, orderby=, groupby={});

    %if %length(%superq(source_table))=0 or %length(%superq(target_table))=0 or
        %length(%superq(orderby))=0 %then %return;

    proc cas;
        session conn;
        table.partition /
            table={
                caslib="casuser",
                name="&source_table.",
                orderby=&orderby.,
                groupby=&groupby.
            },
            casout={
                caslib="casuser",
                name="&target_table.",
                replace=true
            };
    quit;

%mend _gini_sort_cas_as;

%macro _gini_profile_general(data=, split=, target=, vars_num=,
    with_missing=1, out=work._gini_var_profile);

    %local _gini_split_label _gini_var_count;
    %let _gini_split_label=%upcase(%superq(split));
    %let _gini_var_count=%sysfunc(countw(%superq(vars_num), %str( )));

    %if %length(%superq(vars_num))=0 %then %do;
        data &out.;
            length Variable $64 Split $5 N_Total N_Default N_Valid N_Gini 8;
            stop;
        run;
        %return;
    %end;

    data &out.;
        length Variable $64 Split $5 N_Total N_Default N_Valid N_Gini 8;
        retain N_Total 0 N_Default 0;
        array _gini_vars {*} &vars_num.;
        array _gini_valid[&_gini_var_count.] _temporary_;
        array _gini_n[&_gini_var_count.] _temporary_;

        set &data. end=_gini_eof;

        N_Total + 1;
        N_Default + &target.;

        do _gini_i=1 to dim(_gini_vars);
            if not missing(_gini_vars[_gini_i]) then _gini_valid[_gini_i] + 1;
            %if &with_missing.=1 %then %do;
                if not missing(&target.) then _gini_n[_gini_i] + 1;
            %end;
            %else %do;
                if not missing(&target.) and not missing(_gini_vars[_gini_i])
                    then _gini_n[_gini_i] + 1;
            %end;
        end;

        if _gini_eof then do;
            Split=symget('_gini_split_label');
            do _gini_i=1 to dim(_gini_vars);
                Variable=upcase(vname(_gini_vars[_gini_i]));
                N_Valid=_gini_valid[_gini_i];
                N_Gini=_gini_n[_gini_i];
                output;
            end;
        end;

        keep Variable Split N_Total N_Default N_Valid N_Gini;
    run;

%mend _gini_profile_general;

%macro _gini_profile_monthly(data=, split=, target=, vars_num=, byvar=,
    with_missing=1, out=work._gini_var_profile_m);

    %local _gini_split_label _gini_var_count;
    %let _gini_split_label=%upcase(%superq(split));
    %let _gini_var_count=%sysfunc(countw(%superq(vars_num), %str( )));

    %if %length(%superq(vars_num))=0 %then %do;
        data &out.;
            length Variable $64 Split $5 Periodo 8 N_Total N_Default N_Valid
                N_Gini 8;
            stop;
        run;
        %return;
    %end;

    data &out.;
        length Variable $64 Split $5 Periodo 8 N_Total N_Default N_Valid N_Gini 8;
        retain N_Total N_Default;
        array _gini_vars {*} &vars_num.;
        array _gini_valid[&_gini_var_count.] _temporary_;
        array _gini_n[&_gini_var_count.] _temporary_;

        set &data. end=_gini_eof;
        by &byvar.;

        if first.&byvar. then do;
            N_Total=0;
            N_Default=0;
            do _gini_i=1 to dim(_gini_vars);
                _gini_valid[_gini_i]=0;
                _gini_n[_gini_i]=0;
            end;
        end;

        N_Total + 1;
        N_Default + &target.;

        do _gini_i=1 to dim(_gini_vars);
            if not missing(_gini_vars[_gini_i]) then _gini_valid[_gini_i] + 1;
            %if &with_missing.=1 %then %do;
                if not missing(&target.) then _gini_n[_gini_i] + 1;
            %end;
            %else %do;
                if not missing(&target.) and not missing(_gini_vars[_gini_i])
                    then _gini_n[_gini_i] + 1;
            %end;
        end;

        if last.&byvar. then do;
            Split=symget('_gini_split_label');
            Periodo=&byvar.;
            do _gini_i=1 to dim(_gini_vars);
                Variable=upcase(vname(_gini_vars[_gini_i]));
                N_Valid=_gini_valid[_gini_i];
                N_Gini=_gini_n[_gini_i];
                output;
            end;
        end;

        keep Variable Split Periodo N_Total N_Default N_Valid N_Gini;
    run;

%mend _gini_profile_monthly;

%macro _gini_partition_vars(data=, train_data=, oot_data=, vars_num=,
    out_train=_gini_vars_train, out_oot=_gini_vars_oot,
    out_shared=_gini_vars_shared);

    %local _i _var _exists_train _exists_oot _vars_train _vars_oot
        _vars_shared;

    %let _vars_train=;
    %let _vars_oot=;
    %let _vars_shared=;
    %let _i=1;
    %let _var=%scan(&vars_num., &_i., %str( ));

    %if %length(%superq(data)) > 0 %then %do;
        %do %while(%length(&_var.) > 0);
            %_gini_var_exists(data=&data., var=&_var., outvar=_exists_train);

            %if &_exists_train.=1 %then %do;
                %let _vars_train=&_vars_train. &_var.;
                %let _vars_oot=&_vars_oot. &_var.;
                %let _vars_shared=&_vars_shared. &_var.;
            %end;
            %else %do;
                %put WARNING: [gini_variables] Variable &_var. no existe en
                    &data. y sera omitida.;
            %end;

            %let _i=%eval(&_i. + 1);
            %let _var=%scan(&vars_num., &_i., %str( ));
        %end;
    %end;
    %else %do;
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
    %end;

    %let &out_train=%sysfunc(compbl(&_vars_train.));
    %let &out_oot=%sysfunc(compbl(&_vars_oot.));
    %let &out_shared=%sysfunc(compbl(&_vars_shared.));

%mend _gini_partition_vars;

%macro _gini_freqtab_general(data=, target=, score=, with_missing=1,
    out=casuser._gini_freqtab);
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
