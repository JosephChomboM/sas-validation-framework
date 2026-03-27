/* =========================================================================
gini_contract.sas - Validaciones pre-ejecucion del modulo Gini

Verifica:
- input unificado accesible y con observaciones
- target, score, byvar, split y def_cld definidos
- columnas target, score, byvar y split existen
- hay observaciones TRAIN y OOT luego del filtro byvar <= def_cld
- target tiene al menos dos clases por split luego del filtro
========================================================================= */
%macro gini_contract(input_caslib=, input_table=, target=, score=,
    byvar=, def_cld=, split_var=Split);

    %let _gini_rc=0;

    %local _gini_table_exists _gini_nobs_scope _gini_has_col _gini_split_trn
        _gini_split_oot _gini_filt_trn _gini_filt_oot _gini_levels_trn
        _gini_levels_oot;

    %if %length(%superq(input_table))=0 %then %do;
        %put ERROR: [gini_contract] input_table no definida.;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [gini_contract] target no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(score))=0 %then %do;
        %put ERROR: [gini_contract] score no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [gini_contract] byvar no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(split_var))=0 %then %do;
        %put ERROR: [gini_contract] split_var no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [gini_contract] def_cld no definido.;
        %let _gini_rc=1;
        %return;
    %end;

    %let _gini_table_exists=0;
    proc sql noprint;
        select count(*) into :_gini_table_exists trimmed
        from dictionary.tables
        where upcase(libname)=upcase("&input_caslib.")
          and upcase(memname)=upcase("&input_table.");
    quit;

    %if &_gini_table_exists.=0 %then %do;
        %put ERROR: [gini_contract] &input_caslib..&input_table. no existe.;
        %let _gini_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_gini_nobs_scope trimmed
        from &input_caslib..&input_table.;
    quit;

    %if &_gini_nobs_scope.=0 %then %do;
        %put ERROR: [gini_contract] &input_caslib..&input_table. tiene 0 obs.;
        %let _gini_rc=1;
        %return;
    %end;

    %macro _gini_chk_col(col=, label=);
        %let _gini_has_col=0;
        proc sql noprint;
            select count(*) into :_gini_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&input_table.")
              and upcase(name)=upcase("&col.");
        quit;

        %if &_gini_has_col.=0 %then %do;
            %put ERROR: [gini_contract] &label.=&col. no encontrada en
                &input_table..;
            %let _gini_rc=1;
            %return;
        %end;
    %mend _gini_chk_col;

    %_gini_chk_col(col=&target., label=target);
    %if &_gini_rc. ne 0 %then %return;
    %_gini_chk_col(col=&score., label=score);
    %if &_gini_rc. ne 0 %then %return;
    %_gini_chk_col(col=&byvar., label=byvar);
    %if &_gini_rc. ne 0 %then %return;
    %_gini_chk_col(col=&split_var., label=split_var);
    %if &_gini_rc. ne 0 %then %return;

    proc sql noprint;
        select count(*) into :_gini_split_trn trimmed
        from &input_caslib..&input_table.
        where upcase(strip(&split_var.))='TRAIN';

        select count(*) into :_gini_split_oot trimmed
        from &input_caslib..&input_table.
        where upcase(strip(&split_var.))='OOT';

        select count(*) into :_gini_filt_trn trimmed
        from &input_caslib..&input_table.
        where upcase(strip(&split_var.))='TRAIN'
          and &byvar. <= &def_cld.
          and not missing(&target.);

        select count(*) into :_gini_filt_oot trimmed
        from &input_caslib..&input_table.
        where upcase(strip(&split_var.))='OOT'
          and &byvar. <= &def_cld.
          and not missing(&target.);

        select count(distinct &target.) into :_gini_levels_trn trimmed
        from &input_caslib..&input_table.
        where upcase(strip(&split_var.))='TRAIN'
          and &byvar. <= &def_cld.
          and not missing(&target.);

        select count(distinct &target.) into :_gini_levels_oot trimmed
        from &input_caslib..&input_table.
        where upcase(strip(&split_var.))='OOT'
          and &byvar. <= &def_cld.
          and not missing(&target.);
    quit;

    %if &_gini_split_trn.=0 %then %do;
        %put ERROR: [gini_contract] No hay observaciones TRAIN en
            &input_caslib..&input_table..;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_split_oot.=0 %then %do;
        %put ERROR: [gini_contract] No hay observaciones OOT en
            &input_caslib..&input_table..;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_filt_trn.=0 %then %do;
        %put ERROR: [gini_contract] TRAIN sin observaciones con &byvar. <=
            &def_cld.;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_filt_oot.=0 %then %do;
        %put ERROR: [gini_contract] OOT sin observaciones con &byvar. <=
            &def_cld.;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_levels_trn. < 2 %then %do;
        %put ERROR: [gini_contract] TRAIN no tiene al menos dos clases de
            target luego del filtro temporal.;
        %let _gini_rc=1;
        %return;
    %end;

    %if &_gini_levels_oot. < 2 %then %do;
        %put ERROR: [gini_contract] OOT no tiene al menos dos clases de
            target luego del filtro temporal.;
        %let _gini_rc=1;
        %return;
    %end;

    %put NOTE: [gini_contract] OK - target=&target. score=&score.
        byvar=&byvar. def_cld=&def_cld. split_var=&split_var.;
    %put NOTE: [gini_contract] scope=&_gini_nobs_scope. obs,
        TRAIN=&_gini_split_trn. (filtrado=&_gini_filt_trn.),
        OOT=&_gini_split_oot. (filtrado=&_gini_filt_oot.).;

%mend gini_contract;
