from openai import OpenAI

client = OpenAI(api_key="sk-4a130549722c4a9eb4732cb85053b9df", base_url="https://api.deepseek.com")

response = client.chat.completions.create(
    model="deepseek-reasoner",
    messages=[
        {"role": "system", "content": "is 9.8 greater than 9.81 ??"},
        {"role": "user", "content": "Hello"},
    ],
    stream=False
)

print(response.choices[0].message.content)