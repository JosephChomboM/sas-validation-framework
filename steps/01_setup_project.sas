/* =========================================================================
   steps/01_setup_project.sas — Step 1: Setup del proyecto (FRONTEND)

   Este step define la ruta raíz del proyecto.
   La creación física de carpetas se realiza en Step 03.
   ========================================================================= */

/* ---- CONFIGURACIÓN DEL USUARIO (editar aquí) -------------------------- */

/*  _id_project_root  : Ruta absoluta al directorio raíz del proyecto       */
/*                       en el filesystem accesible por el servidor CAS.     */
/*                       Tipo: texto (ruta de filesystem)                    */
/*                       Ejemplo: /data/projects/framework_validacion        */
%let fw_root = /path/to/framework_validacion;

%put NOTE: ======================================================;
%put NOTE: [step-01] fw_root = &fw_root.;
%put NOTE: [step-01] Ruta del proyecto configurada. Carpeta se crea en Step 03.;
%put NOTE: ======================================================;
