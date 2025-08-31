using WebPush;

var keys = VapidHelper.GenerateVapidKeys();
Console.WriteLine("PublicKey:  " + keys.PublicKey);
Console.WriteLine("PrivateKey: " + keys.PrivateKey);
