/* =========================================================================
   steps/04_select_methods.sas — Step 4: Selección de métodos (FRONTEND)

   El usuario configura aquí qué módulos de validación ejecutar
   y un label descriptivo opcional para el run.

   Ref: design.md §5.1, module_catalog.md
   ========================================================================= */

/* ---- CONFIGURACIÓN DEL USUARIO (editar aquí) -------------------------- */

/*  _id_methods_select     : Módulos a ejecutar (espacio-separado)          */
/*                            Valores posibles: gini psi (extensible)        */
/*                            Tipo: texto ($500)                             */
%let methods_list = gini psi;

/*  _id_run_label          : Etiqueta descriptiva opcional del run          */
/*                            Tipo: texto libre ($200)                       */
/*                            Ejemplo: "validacion_mensual_Q1_2026"          */
%let run_label = ;

/* ---- LOG DE CONFIRMACIÓN ---------------------------------------------- */

%put NOTE: ======================================================;
%put NOTE: [step-04] Selección de métodos:;
%put NOTE:   methods_list = &methods_list.;
%put NOTE:   run_label    = &run_label.;
%put NOTE: ======================================================;
