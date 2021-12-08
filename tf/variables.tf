variable "bucket" {
  description = "Name of the bucket"
}

variable "alb_security_group" {
  default = "sg-06d069fbc34012849"
}

variable "vpc_id" {
  default = "vpc-04b176d1264698ffc"
}
variable "is_alb_public" {
  default = true
}

variable "alb_subnets" {
  type = list(string)
  default = [
    "subnet-0d0efc3629a6bcfb8",
    "subnet-0f2c99503a5d23938",
    "subnet-075e8ca43ee291142"
  ]
}
variable "alb_ssl_policy" {
  default = "ELBSecurityPolicy-2016-08"
}

variable "ecs_service_security_group" {
  default = "sg-08cd34d4e02699119"
}

variable "ecs_service_subnets" {
  type = list(string)

  default = [
    "subnet-0296c453ecf63a7ae",
    "subnet-0445769f0b325373f",
    "subnet-0bbf2a9cdee0ce681"
  ]
}

variable "task_role_arn" {
  default = "arn:aws:iam::977611293394:role/ecsTaskExecutionRole"
}
variable "task_execution_role_arn" {
  default = "arn:aws:iam::977611293394:role/ecsTaskExecutionRole"
}
