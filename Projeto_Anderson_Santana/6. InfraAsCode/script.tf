resource "aws_glue_job" "projeto_data_engineer" {
  name     = "job-validacao-cliente"
  role_arn = aws_iam_role.glue_role.arn

  # Especificações conforme solicitado
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 10

  command {
    # Padrão AWS: bucket de assets/scripts seguido pelo nome do arquivo
    script_location = "s3://aws-glue-assets-123456789012-us-east-1/scripts/script.py"
    python_version  = "3.11"
  }

  default_arguments = {
    "--TempDir" = "s3://aws-glue-temp-123456789012-us-east-1/temp/"
  }

  tags = {
    "Nome"  = "projeto"
    "Valor" = "teste_eng_dados"
  }
}