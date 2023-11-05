using MassTransit;
using MSA.Common.Contracts.Domain;
using MSA.Common.Contracts.Domain.Commands.Product;
using MSA.Common.Contracts.Domain.Events.Product;
using MSA.ProductService.Entities;

namespace MSA.ProductService.Consumers;

public class ValidateProductConsumer : IConsumer<ValidateProduct>
{
    private readonly ILogger<ValidateProductConsumer> logger;
    private readonly IRepository<Product> repository;
    private readonly IPublishEndpoint publishEndpoint;
    public ValidateProductConsumer(
        ILogger<ValidateProductConsumer> logger,
        IRepository<Product> repository,
        IPublishEndpoint publishEndpoint)
    {
        this.logger = logger;
        this.repository = repository;
        this.publishEndpoint = publishEndpoint;
    }

    public async Task Consume(ConsumeContext<ValidateProduct> context)
    {
        var message = context.Message;

        logger.Log(LogLevel.Information,
            $"Receiving message of order {message.OrderId} validating product {message.ProductId}"
        );

        var product = await repository.GetAsync(message.ProductId);

        if (product != null)
        {
            await publishEndpoint.Publish(new ProductValidatedSucceeded
            {
                ProductId = message.ProductId,
                OrderId = message.OrderId
            });

            logger.Log(LogLevel.Error,
                $"Message of order {message.OrderId} product {message.ProductId} is exist"
            );
        }
        else
        {
            await publishEndpoint.Publish(new ProductValidatedFailed
            {
                ProductId = message.ProductId,
                OrderId = message.OrderId,
                Reason = "Product not exist"
            });

            logger.Log(LogLevel.Information,
                $"Message of order {message.OrderId} product {message.ProductId} is not exist"
            );
        }
    }
}