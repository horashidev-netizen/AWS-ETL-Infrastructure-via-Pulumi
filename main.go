package main

import (
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/lambda"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/s3"
	"github.com/pulumi/pulumi-aws/sdk/v6/go/aws/sqs"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		// 1. Tạo S3 Bucket để chứa file CSV
		bucket, err := s3.NewBucketV2(ctx, "horashi-movies-data", nil)
		if err != nil {
			return err
		}

		// 2. Tạo SQS Queue làm hàng đợi
		queue, err := sqs.NewQueue(ctx, "movie-etl-queue", &sqs.QueueArgs{
			VisibilityTimeoutSeconds: pulumi.Int(300), // Cho phép Lambda Consumer xử lý tối đa 5 phút
		})
		if err != nil {
			return err
		}

		// 3. Tạo IAM Role cho Lambda (Quyền thực thi cơ bản)
		lambdaRole, err := iam.NewRole(ctx, "etlLambdaRole", &iam.RoleArgs{
			AssumeRolePolicy: pulumi.String(`{
				"Version": "2012-10-17",
				"Statement": [{
					"Action": "sts:AssumeRole",
					"Principal": {
						"Service": "lambda.amazonaws.com"
					},
					"Effect": "Allow",
					"Sid": ""
				}]
			}`),
		})
		if err != nil {
			return err
		}

		// Đính kèm Policy cơ bản (Ghi log vào CloudWatch)
		_, err = iam.NewRolePolicyAttachment(ctx, "lambdaBasicExecution", &iam.RolePolicyAttachmentArgs{
			Role:      lambdaRole.Name,
			PolicyArn: pulumi.String("arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"),
		})

		// Đính kèm quyền Đọc S3 và Quyền SQS cho Role
		_, err = iam.NewRolePolicy(ctx, "lambdaS3SqsPolicy", &iam.RolePolicyArgs{
			Role: lambdaRole.Name,
			Policy: pulumi.String(`{
				"Version": "2012-10-17",
				"Statement": [
					{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "*"},
					{"Effect": "Allow", "Action": ["sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], "Resource": "*"}
				]
			}`),
		})

		// 4. Tạo Lambda 1 (Producer) - Đọc S3 ném vào SQS
		producerLambda, err := lambda.NewFunction(ctx, "producerLambda", &lambda.FunctionArgs{
			Runtime: pulumi.String("python3.10"),
			Code:    pulumi.NewFileArchive("./producer"),
			Handler: pulumi.String("lambda_function.lambda_handler"),
			Role:    lambdaRole.Arn,
			Timeout: pulumi.Int(60), // Chạy tối đa 60 giây
			Environment: &lambda.FunctionEnvironmentArgs{
				Variables: pulumi.StringMap{
					"QUEUE_URL": queue.Url,
				},
			},
		})

		// Gắn Trigger: Khi có file tải lên S3 -> Gọi Lambda 1
		// Lưu ý: S3 bucket notification cần phân quyền resource-based policy cho S3 gọi Lambda
		allowBucket, err := lambda.NewPermission(ctx, "allowBucketTrigger", &lambda.PermissionArgs{
			Action:    pulumi.String("lambda:InvokeFunction"),
			Function:  producerLambda.Arn,
			Principal: pulumi.String("s3.amazonaws.com"),
			SourceArn: bucket.Arn,
		})
		if err != nil {
			return err
		}

		_, err = s3.NewBucketNotification(ctx, "bucketNotification", &s3.BucketNotificationArgs{
			Bucket: bucket.ID(),
			LambdaFunctions: s3.BucketNotificationLambdaFunctionArray{
				&s3.BucketNotificationLambdaFunctionArgs{
					LambdaFunctionArn: producerLambda.Arn,
					Events:            pulumi.StringArray{pulumi.String("s3:ObjectCreated:*")},
					FilterSuffix:      pulumi.String(".csv"), // Chỉ kích hoạt với file CSV
				},
			},
		}, pulumi.DependsOn([]pulumi.Resource{allowBucket}))

		// 5. Tạo Lambda 2 (Consumer) - Gọi AI & Lưu Database
		// Lấy biến môi trường từ config của Pulumi
		conf := config.New(ctx, "")
		mongoUri := conf.Require("mongoUri")
		hfToken := conf.Require("hfToken")

		consumerLambda, err := lambda.NewFunction(ctx, "consumerLambda", &lambda.FunctionArgs{
			Runtime: pulumi.String("python3.10"),
			Code:    pulumi.NewFileArchive("./consumer"),
			Handler: pulumi.String("lambda_function.lambda_handler"),
			Role:    lambdaRole.Arn,
			Timeout: pulumi.Int(120),
			Environment: &lambda.FunctionEnvironmentArgs{
				Variables: pulumi.StringMap{
					"MONGO_URI": pulumi.String(mongoUri),
					"HF_TOKEN":  pulumi.String(hfToken),
				},
			},
		})

		// Gắn Trigger: SQS nhận tin nhắn -> Gọi Lambda 2
		_, err = lambda.NewEventSourceMapping(ctx, "sqsTrigger", &lambda.EventSourceMappingArgs{
			EventSourceArn: queue.Arn,
			FunctionName:   consumerLambda.Arn,
			BatchSize:      pulumi.Int(5), // Mỗi lần bốc 5 phim để xử lý
		})
		if err != nil {
			return err
		}

		// Xuất tên Bucket ra màn hình để bạn biết chỗ upload file
		ctx.Export("bucketName", bucket.ID())
		return nil
	})
}