class Chatbox {

  constructor() {
    this.args = {
      openButton: document.querySelector(".chatbox__button"),
      chatBox: document.querySelector(".chatbox__support"),
      sendButton: document.querySelector(".send__button"),
    };

    this.state = false;
    this.messages = [];
    this.initialMessage = [];
    this.messages.push({ name: "Sam", message: "Hi there! I'm Perry." },{ name: "Sam", message: "I'm an AI detective who performs multi-web product searching, comparing and filtering to bring you, your desired products hands-on." },{ name: "Sam", message: "So from now, no need to go on product hunt. Just have a chat with Perry." },{ name: "Sam", message: "To make me work, type," },{ name: "Sam", message: "<b>Show the best products for {<i>product name</i>}</b>" });
  }

  display() {
    const { openButton, chatBox, sendButton } = this.args;
    sendButton.addEventListener("click", () => this.onSendButton(chatBox));
    const node = chatBox.querySelector("input");
    node.addEventListener("keyup", ({ key }) => {
      if (key === "Enter") {
        this.onSendButton(chatBox);
      }
    });
  }

  toggleState(chatbox) {
    this.state = !this.state;
    if (this.state) {
      chatbox.classList.add("chatbox--active");
    } else {
      chatbox.classList.remove("chatbox--active");
    }
  }

  onSendButton(chatbox) {
    let chatResponseFlag = true;
    var textField = chatbox.querySelector("input");
    let text1 = textField.value;
    if (text1 === "") {
      return;
    }

    let msg1 = { name: "User", message: text1 };
    this.messages.push(msg1);
    this.updateChatText(chatbox);
    textField.value = "";
    this.loading = document.getElementById("loading");
    this.loading.style.display = "block";

    fetch("http://127.0.0.1:5000/predict", {
      method: "POST",
      body: JSON.stringify({ message: text1 }),
      mode: "cors",
      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((r) => r.json())
      .then((r) => {
        let msg2 = { name: "Sam", message: r.answer };
        if (r != undefined) this.loading.style.display = "none";
        this.messages.push(msg2);
        this.updateChatText(chatbox);
        textField.value = "";
      })
      .catch((error) => {
        console.error("Error:", error);
        this.updateChatText(chatbox);
        textField.value = "";
      });
  }
  

  updateChatText(chatbox) {
    var html = "";
    this.messages
      .slice()
      .reverse()
      .forEach(function (item, index) {
        if (item.name === "Sam") {
          html +=
            '<div class="messages__item messages__item--visitor">' +
            item.message +
            "</div>";
        } else {
          html +=
            '<div class="messages__item messages__item--operator">' +
            item.message +
            "</div>";
        }
      });

    const chatmessage = chatbox.querySelector(".chatbox__messages");
    chatmessage.innerHTML = html;
  }
}

const chatbox = new Chatbox();
chatbox.display();


