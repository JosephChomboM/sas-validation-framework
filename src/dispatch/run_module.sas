/* =========================================================================
run_module.sas - Dispatch: ejecuta un modulo en un contexto dado

Tres modos de operacion:

A) Single-input (dual_input=0, scope_input=0, default):
Ciclo: resolver path persistente -> promote base completa -> derivar split
(_active_input) -> ejecutar -> drop.
Para modulos que operan sobre un solo dataset (correlacion, gini, etc.).
El modulo recibe: input_table=_active_input, split=<train|oot>.

B) Dual-input (dual_input=1):
Ciclo: resolver path persistente -> promote base completa -> derivar
_train_input + _oot_input -> ejecutar -> drop.
Para modulos que comparan TRAIN vs OOT simultaneamente (PSI, etc.).
El modulo recibe: train_table=_train_input, oot_table=_oot_input.
El parametro split= se ignora (siempre deriva ambos).

C) Scope-input (scope_input=1):
Ciclo: resolver path persistente -> promote base completa (_scope_input) ->
ejecutar -> drop.
Para modulos que necesitan la base persistente unificada y derivan su logica
interna desde esa tabla (Universe, u otros modulos CAS-first comparativos).
El modulo recibe: input_table=_scope_input.
Los parametros split= y dual_input= se ignoran.

Parametros:
module      = nombre del modulo (correlacion, psi, gini, etc.)
troncal_id  = id numerico de troncal
split       = train | oot (ignorado si dual_input=1 o scope_input=1)
seg_id      = id numerico de segmento (vacio = universo/base)
run_id      = identificador del run actual
dual_input  = 0 (default) | 1
scope_input = 0 (default) | 1

CASLIB policy:
- Input persistente se lee desde CASLIB PROC (PATH -> data/processed/,
  subdirs=1).
- Los splits TRAIN/OOT se derivan en ejecución usando
  casuser.cfg_troncales.
- Output se escribe en CASLIB OUT o en un CASLIB scoped del
modulo (ej. MOD_GINI_<run_id>), siguiendo caslib_lifecycle.md.
- casuser NO se usa para datos operativos.
- CASLIBs PROC y OUT deben estar creados antes de llamar esta macro.

Convencion: el modulo debe exponer %<module>_run(...).
========================================================================= */
%macro _run_module_prepare_scope(troncal_id=, seg_id=, input_path_var=_input_path);

    %local _scope_loaded;
    %let _run_module_scope_rc=0;

    %fw_path_processed(outvar=&input_path_var., troncal_id=&troncal_id.,
        seg_id=&seg_id.);

    %_fw_load_scope_input(troncal_id=&troncal_id., seg_id=&seg_id.,
        input_caslib=PROC, output_caslib=PROC, output_table=_scope_input,
        sess=conn);

    %let _scope_loaded=0;
    proc sql noprint;
        select count(*) into :_scope_loaded trimmed
        from dictionary.tables
        where upcase(libname)='PROC'
          and upcase(memname)='_SCOPE_INPUT';
    quit;

    %if &_scope_loaded.=0 %then %do;
        %put ERROR:=====================================================;
        %put ERROR: [run_module] No se pudo cargar _scope_input.;
        %put ERROR: [run_module] troncal=&troncal_id.;
        %put ERROR: [run_module] seg_id=&seg_id.;
        %put ERROR: [run_module] source=&&&input_path_var.;
        %put ERROR: [run_module] Verifique que el .sashdat existe en
            data/processed/.;
        %put ERROR:=====================================================;
        %let _run_module_scope_rc=1;
    %end;

%mend _run_module_prepare_scope;

%macro _run_module_cleanup_proc(mode=);

    proc cas;
        session conn;
        table.dropTable / caslib="PROC" name="_scope_input" quiet=true;
        %if %upcase(&mode.)=SINGLE %then %do;
            table.dropTable / caslib="PROC" name="_active_input" quiet=true;
        %end;
        %else %if %upcase(&mode.)=DUAL %then %do;
            table.dropTable / caslib="PROC" name="_train_input" quiet=true;
            table.dropTable / caslib="PROC" name="_oot_input" quiet=true;
        %end;
    quit;

%mend _run_module_cleanup_proc;

%macro run_module(module=, troncal_id=, split=, seg_id=, run_id=, dual_input=0,
    scope_input=0);

	/* ---- Locals -------------------------------------------------------- */
	%local _scope _out_caslib _promote_ok _input_path _run_module_scope_rc;

	/* Construir scope label para naming de outputs */
	%if %superq(seg_id)=%then %let _scope=base;
	%else %let _scope=seg%sysfunc(putn(&seg_id., z3.));

	%let _out_caslib=OUT;

	%if &scope_input.=1 %then %do;

		/* =================================================================
		MODO C: Scope-input (universe y modulos que operan sobre la base
		persistente unificada)
		================================================================= */
		%put NOTE: -----------------------------------------------------;
		%put NOTE: [run_module] module=&module. troncal=&troncal_id.
			seg_id=&seg_id. (scope_input);
		%put NOTE: -----------------------------------------------------;

		/* 1) Resolver input persistente unico y promover scope */
        %_run_module_prepare_scope(troncal_id=&troncal_id., seg_id=&seg_id.,
            input_path_var=_input_path);

        %if &_run_module_scope_rc.=1 %then %do;
			%put ERROR: [run_module] module=&module.;
            %return;
        %end;

		/* 3) Ejecutar modulo scope-input */
		%include "&fw_root./src/modules/&module./&module._run.sas";
		%if %upcase(&module.)=CORRELACION %then %do;
			%&module._run( input_caslib=PROC, input_table=_scope_input,
				output_caslib=&_out_caslib., troncal_id=&troncal_id.,
				split=&split., scope=&_scope., run_id=&run_id. );
		%end;
		%else %do;
			%&module._run( input_caslib=PROC, input_table=_scope_input,
				output_caslib=&_out_caslib., troncal_id=&troncal_id.,
				scope=&_scope., run_id=&run_id. );
		%end;

		%put NOTE: [run_module] &module. completado para
			troncal_&troncal_id./&_scope. (scope-input).;

		/* 4) Drop tabla temporal */
        %_run_module_cleanup_proc(mode=SCOPE);

	%end;
	%else %if &dual_input.=0 %then %do;

		/* =================================================================
		MODO A: Single-input (correlacion, gini, etc.)
		================================================================= */
		%put NOTE: -----------------------------------------------------;
		%put NOTE: [run_module] module=&module. troncal=&troncal_id.
			split=&split. seg_id=&seg_id.;
		%put NOTE: -----------------------------------------------------;

		/* 1) Resolver input persistente unico y promover scope */
        %_run_module_prepare_scope(troncal_id=&troncal_id., seg_id=&seg_id.,
            input_path_var=_input_path);

        %if &_run_module_scope_rc.=1 %then %do;
			%put ERROR: [run_module] module=&module.;
            %return;
        %end;

		/* 2) Derivar split en memoria */
        %_fw_build_split_table(troncal_id=&troncal_id., split=&split.,
            source_caslib=PROC, source_table=_scope_input,
            target_caslib=PROC, target_table=_active_input, sess=conn);

		/* Validar promote (fail-fast) */
		%let _promote_ok=0;

		proc sql noprint;
			select count(*) into :_promote_ok trimmed from dictionary.tables
				where upcase(libname)='PROC' and upcase(memname)=
				'_ACTIVE_INPUT';
		quit;

		%if &_promote_ok.=0 %then %do;
			%put ERROR:=====================================================;
			%put ERROR: [run_module] No se pudo cargar _active_input.;
			%put ERROR: [run_module] module=&module.;
			%put ERROR: [run_module] troncal=&troncal_id.;
			%put ERROR: [run_module] split=&split.;
			%put ERROR: [run_module] seg_id=&seg_id.;
			%put ERROR: [run_module] source=&_input_path.;
            %put ERROR: [run_module] split=&split. derivado dinamicamente desde cfg_troncales.;
			%put ERROR: [run_module] Verifique que el .sashdat existe en
				data/processed/.;
			%put ERROR:=====================================================;
			%return;
		%end;

		/* 3) Ejecutar modulo single-input */
		%include "&fw_root./src/modules/&module./&module._run.sas";
		%&module._run( input_caslib=PROC, input_table=_active_input,
			output_caslib=&_out_caslib., troncal_id=&troncal_id., split=&split.,
			scope=&_scope., run_id=&run_id. );

		%put NOTE: [run_module] &module. completado para
			troncal_&troncal_id./&split./&_scope.;

		/* 4) Drop tablas temporales */
        %_run_module_cleanup_proc(mode=SINGLE);

	%end;
	%else %do;

		/* =================================================================
		MODO B: Dual-input (PSI, etc.) - promueve train + oot
		El parametro split= se ignora.
		================================================================= */
		%put NOTE: -----------------------------------------------------;
		%put NOTE: [run_module] module=&module. troncal=&troncal_id.
			seg_id=&seg_id. (dual_input);
		%put NOTE: -----------------------------------------------------;

		/* 1) Resolver input persistente único */
        %_run_module_prepare_scope(troncal_id=&troncal_id., seg_id=&seg_id.,
            input_path_var=_input_path);

        %if &_run_module_scope_rc.=1 %then %do;
			%put ERROR: [run_module] module=&module.;
            %return;
        %end;

		/* 2) Derivar TRAIN + OOT */
        %_fw_build_split_table(troncal_id=&troncal_id., split=TRAIN,
            source_caslib=PROC, source_table=_scope_input,
            target_caslib=PROC, target_table=_train_input, sess=conn);
        %_fw_build_split_table(troncal_id=&troncal_id., split=OOT,
            source_caslib=PROC, source_table=_scope_input,
            target_caslib=PROC, target_table=_oot_input, sess=conn);

		/* Validar promote de ambas tablas */
		%let _promote_ok=0;

		proc sql noprint;
			select count(*) into :_promote_ok trimmed from dictionary.tables
				where upcase(libname)='PROC' and upcase(memname) in
				('_TRAIN_INPUT', '_OOT_INPUT');
		quit;

		%if &_promote_ok. < 2 %then %do;
			%put ERROR:=====================================================;
			%put ERROR: [run_module] No se pudieron cargar _train_input y/o
				_oot_input.;
			%put ERROR: [run_module] module=&module.;
			%put ERROR: [run_module] troncal=&troncal_id.;
			%put ERROR: [run_module] seg_id=&seg_id.;
			%put ERROR: [run_module] source=&_input_path.;
            %put ERROR: [run_module] TRAIN/OOT se derivan dinamicamente desde cfg_troncales.;
			%put ERROR: [run_module] Verifique que el .sashdat existe en
				data/processed/.;
			%put ERROR:=====================================================;
			%return;
		%end;

		/* 3) Ejecutar modulo dual-input */
		%include "&fw_root./src/modules/&module./&module._run.sas";
		%&module._run( input_caslib=PROC, train_table=_train_input, oot_table=
			_oot_input, output_caslib=&_out_caslib., troncal_id=&troncal_id.,
			scope=&_scope., run_id=&run_id. );

		%put NOTE: [run_module] &module. completado para
			troncal_&troncal_id./&_scope. (dual-input).;

		/* 4) Drop tablas temporales */
        %_run_module_cleanup_proc(mode=DUAL);

	%end;

%mend run_module;
