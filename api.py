import requests

API_URL = "https://api-inference.huggingface.co/models/facebook/blenderbot-400M-distill"
headers = {"Authorization": "Bearer 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"}

def query(payload):
	response = requests.post(API_URL, headers=headers, json=payload)
	return response.json()

text = input()

output = query({
	"inputs": {
		"text": text
	},
})

print(output['generated_text'])
