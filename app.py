class Door:
    def __init__(self, color: str, size: int):
        self.color = color
        self.size = size

    def set_color(self, color:str)-> None:
        self.color = color
        print(f"The door is color: {color}")

    def set_size(self, size: int)-> None:
        print(f"The door is of size: {self.size}")

    def close(self)-> None:
        print(f"The door of size {self.size} and color: {self.color} is closed")

    def open(self)-> None:
        print(f"The door of size {self.size} and color: {self.color} is open")

    def lock(self)-> None:
        print(f"The door of size {self.size} and color: {self.color} is locked")
    



class Window(Door):
    
    def __init__(self, color: str, size: int, tint_color: str = None):
        super().__init__(color, size)
        self.tint_color = tint_color

    def tint(self, tint_color: str):
        print(f"The window is tinted {tint_color}")

    def set_tint_color(self):
        tint_color = input(f"Please enter the tint color of your window: ")
        self.tint_color = tint_color
        self.tint(tint_color)



def main():
    print("Hello and welcome to the door app")
    
    gbolahan_door = Door(color='red', size=100)

    gbolahan_door.open()
    gbolahan_door.close()
    gbolahan_door.lock()

    gbolly_window = Window(color='blue', size=50)
    gbolly_window.set_tint_color()
    gbolly_window.open()
    gbolly_window.close()
    gbolly_window.lock()


if __name__ == "__main__":
    main()
