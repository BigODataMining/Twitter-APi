using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tweetinvi;

namespace Twitter_API
{  
	class Program
	{
		static void Main(string[] arg2s)
		{
			String BookerList = "localhost:9092";
			String topicName = "top";
			String message = "";
			int count = 0;
			var config = new Dictionary<String, Object> { { "bootstrap.servers",BookerList} };
			Producer<Null, string> Twitter = new Producer<Null, string>(config,null,new StringSerializer(Encoding.UTF8));
			Auth.SetUserCredentials("qp2QrWpeRFUZ8lje8ygdMKTa1", "1RON0DIxnpOzdM5KNo3hexhaKPbvEhVOuysXfJXNbgOYFdI0Ha",
				"925373306001166336-GD1hZXwNaUGf8TKCLdp77yvAtsnT60r", "TocvFziBfn5T62I4RjyNFJ16VadANn81n2a7QYbdy7nR0");
			
			StreamWriter sw = new StreamWriter(@"C:\Twitter_data\data4.txt");
			while (true)
			{
			var stream = Tweetinvi.Stream.CreateSampleStream(); 
				stream.TweetReceived += (sender, args) =>
				{
				// Do what you want with the Tweet
				Console.WriteLine(args.Tweet);
					message = args.Tweet.ToString();
					var deliveryReport = Twitter.ProduceAsync(topicName, null, message);
					sw.WriteLine(message);
					deliveryReport.ContinueWith(task =>
					{
						Console.WriteLine("Status{0}---Partition:{1}, offset: {2}", task.Result.Error, task.Result.Partition, task.Result.Offset);

					});
				};
				count++;stream.StartStream();
				if (count.Equals(2)) { break; }
			}
			sw.Close();

		}
	}
}
