/* =========================================================================
fillrate_contract.sas - Validaciones pre-ejecucion del modulo Fillrate

Verifica:
1) Al menos una lista de variables (num o cat) no vacia
2) byvar definido
3) def_cld definido
4) target definido si hay vars_num (para fillrate vs gini)
5) TRAIN y OOT accesibles y con observaciones
6) byvar existe en ambas tablas
7) target existe en ambas tablas si aplica
8) TRAIN y OOT tienen observaciones luego del filtro byvar <= def_cld

Setea &_fill_rc:
0 = OK, 1 = fallo
========================================================================= */
%macro fillrate_contract(input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=, byvar=, target=, def_cld=);

    %let _fill_rc=0;

    %local _fill_nobs_trn _fill_nobs_oot _fill_has_col _fill_need_target
        _fill_nobs_trn_filt _fill_nobs_oot_filt;

    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [fillrate_contract] No se proporcionaron variables
            numericas ni categoricas.;
        %let _fill_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [fillrate_contract] Variable temporal (byvar) no definida.;
        %let _fill_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [fillrate_contract] def_cld no definida.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_need_target=0;
    %if %length(%superq(vars_num)) > 0 %then %let _fill_need_target=1;

    %if &_fill_need_target.=1 and %length(%superq(target))=0 %then %do;
        %put ERROR: [fillrate_contract] target no definido para el calculo de
            Gini en variables numericas.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_nobs_trn=0;
    proc sql noprint;
        select count(*) into :_fill_nobs_trn trimmed from
            &input_caslib..&train_table.;
    quit;
    %if &_fill_nobs_trn.=0 %then %do;
        %put ERROR: [fillrate_contract] TRAIN &input_caslib..&train_table. no
            accesible o 0 obs.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_nobs_oot=0;
    proc sql noprint;
        select count(*) into :_fill_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;
    %if &_fill_nobs_oot.=0 %then %do;
        %put ERROR: [fillrate_contract] OOT &input_caslib..&oot_table. no
            accesible o 0 obs.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_has_col=0;
    proc sql noprint;
        select count(*) into :_fill_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and
            upcase(memname)=upcase("&train_table.") and
            upcase(name)=upcase("&byvar.");
    quit;
    %if &_fill_has_col.=0 %then %do;
        %put ERROR: [fillrate_contract] byvar=&byvar. no encontrada en TRAIN.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_has_col=0;
    proc sql noprint;
        select count(*) into :_fill_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and
            upcase(memname)=upcase("&oot_table.") and
            upcase(name)=upcase("&byvar.");
    quit;
    %if &_fill_has_col.=0 %then %do;
        %put ERROR: [fillrate_contract] byvar=&byvar. no encontrada en OOT.;
        %let _fill_rc=1;
        %return;
    %end;

    %if &_fill_need_target.=1 %then %do;
        %let _fill_has_col=0;
        proc sql noprint;
            select count(*) into :_fill_has_col trimmed from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.") and
                upcase(memname)=upcase("&train_table.") and
                upcase(name)=upcase("&target.");
        quit;
        %if &_fill_has_col.=0 %then %do;
            %put ERROR: [fillrate_contract] target=&target. no encontrada en
                TRAIN.;
            %let _fill_rc=1;
            %return;
        %end;

        %let _fill_has_col=0;
        proc sql noprint;
            select count(*) into :_fill_has_col trimmed from dictionary.columns
                where upcase(libname)=upcase("&input_caslib.") and
                upcase(memname)=upcase("&oot_table.") and
                upcase(name)=upcase("&target.");
        quit;
        %if &_fill_has_col.=0 %then %do;
            %put ERROR: [fillrate_contract] target=&target. no encontrada en
                OOT.;
            %let _fill_rc=1;
            %return;
        %end;
    %end;

    %let _fill_nobs_trn_filt=0;
    proc sql noprint;
        select count(*) into :_fill_nobs_trn_filt trimmed from
            &input_caslib..&train_table. where &byvar. <= &def_cld.;
    quit;
    %if &_fill_nobs_trn_filt.=0 %then %do;
        %put ERROR: [fillrate_contract] TRAIN no tiene observaciones con
            &byvar. <= &def_cld.;
        %let _fill_rc=1;
        %return;
    %end;

    %let _fill_nobs_oot_filt=0;
    proc sql noprint;
        select count(*) into :_fill_nobs_oot_filt trimmed from
            &input_caslib..&oot_table. where &byvar. <= &def_cld.;
    quit;
    %if &_fill_nobs_oot_filt.=0 %then %do;
        %put ERROR: [fillrate_contract] OOT no tiene observaciones con
            &byvar. <= &def_cld.;
        %let _fill_rc=1;
        %return;
    %end;

    %put NOTE: [fillrate_contract] OK - TRAIN=&_fill_nobs_trn. obs,
        OOT=&_fill_nobs_oot. obs.;
    %put NOTE: [fillrate_contract] TRAIN filtrado=&_fill_nobs_trn_filt.
        OOT filtrado=&_fill_nobs_oot_filt.;
    %put NOTE: [fillrate_contract] byvar=&byvar. target=&target.
        def_cld=&def_cld.;

%mend fillrate_contract;
